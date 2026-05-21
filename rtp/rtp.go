package rtp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sip-ai-bridge/ws"
	"strconv"
	"sync"

	"github.com/coder/websocket"
	"github.com/rs/zerolog/log"
)

var Listeners map[string]*RtpListener
var ListenersMutex sync.RWMutex

type RtpListener struct {
	RemoteAddr *net.UDPAddr
	LocalAddr  *net.UDPAddr
	Udp        *net.UDPConn
	Ws         *websocket.Conn
	AudioChunk []byte
}

func CreateRtpListener(remoteIp, remotePort string) (*RtpListener, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", remoteIp+":"+remotePort)
	if err != nil {
		return nil, err
	}

	//TODO: implement a udp port range and pick a random free port
	localAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(30123))
	if err != nil {
		return nil, err
	}

	udpSocket, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, err
	}

	//TODO: this is super slow, move this into a goroutine somehow, otherwise we get mutiple INVITES, or maybe use a provisional response?
	ws, err := ws.CreateWebSocket()
	if err != nil {
		return nil, err
	}

	rtpListener := RtpListener{
		RemoteAddr: remoteAddr,
		LocalAddr:  localAddr,
		Udp:        udpSocket,
		Ws:         ws,
	}

	return &rtpListener, nil
}

func DeleteRtpListener(callId string) (RtpListener, error) {
	ListenersMutex.RLock()

	if Listeners[callId] == nil {
		//if _, ok := Listeners[callId]; !ok {
		ListenersMutex.RUnlock()
		return RtpListener{}, errors.New("no rtp listener found for the call-id")
	}

	Listeners[callId].Udp.Close()
	ws.DeleteWebSocket(Listeners[callId].Ws)

	listener := Listeners[callId]
	ListenersMutex.RUnlock()

	ListenersMutex.Lock()
	defer ListenersMutex.Unlock()

	delete(Listeners, callId)
	return *listener, nil
}

func ReadRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	for {
		ListenersMutex.RLock()
		listener := Listeners[callId]
		ListenersMutex.RUnlock()

		if listener == nil {
			//log.Info().Msgf("udp socket closed, stopping read")
			return
		}

		resBuffer := make([]byte, 172)
		_, _, err := listener.Udp.ReadFromUDP(resBuffer)
		//_, remoteAddr, err := Listeners[callId].Udp.ReadFromUDP(resBuffer)
		if err != nil {
			//log.Error().Err(err).Msgf("failed to read from the udp socket")
			continue
		}

		//TODO: make this PCMU extraction more robust, it's very basic lol
		if len(resBuffer) >= 12 {
			resBuffer = resBuffer[12:]
		}

		//TODO: checkout rtp packet header/payload format to extract PCMU/PCMA audio data
		//if !strings.HasPrefix(string(reqBuffer), "rtp signature") {
		//continue
		//}

		//log.Debug().Msgf("received payload from udp the socket, payload: %s", string(reqBuffer))
		//fmt.Printf("received payload from the udp socket, si-sp: %s:%d, payload: \n%+v\n", remoteAddr.IP.String(), remoteAddr.Port, resBuffer)

		listener.AudioChunk = append(listener.AudioChunk, resBuffer...)

		if len(listener.AudioChunk) >= 800 {
			err = ws.WriteToWebSocket(listener.Ws, listener.AudioChunk)
			if err != nil {
				log.Error().Err(err).Msgf("failed to write to the websocket")
				continue
			}

			ListenersMutex.Lock()
			Listeners[callId].AudioChunk = []byte{}
			ListenersMutex.Unlock()
		}

		ListenersMutex.Lock()
		Listeners[callId].AudioChunk = append(listener.AudioChunk, resBuffer...)
		ListenersMutex.Unlock()
	}
}

func ReadStt(callId string) {
	defer ws.DeleteWebSocket(Listeners[callId].Ws)

	for {
		ListenersMutex.RLock()
		listener := Listeners[callId]
		ListenersMutex.RUnlock()

		if listener == nil {
			return
		}

		res, err := ws.ReadFromWebSocket(listener.Ws)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the websocket")
			continue
		}

		type CartesiaSttResponse struct {
			Duration float64 `json:"duration"`
			//IsFinal  bool   `json:"is_final"`
			Text string `json:"text"`
		}

		sttRes := CartesiaSttResponse{}
		err = json.Unmarshal(res, &sttRes)
		if err != nil {
			log.Error().Err(err).Msgf("failed to unmarshal the response from the websocket")
			continue
		}

		log.Info().Float64("duration", sttRes.Duration).Str("text", sttRes.Text).Msg("received payload from the websocket")

		//will write to llm stream
		//err = ws.WriteToWebSocket(Listeners[callId].Ws, resBuffer)
		//if err != nil {
		//	log.Error().Err(err).Msgf("failed to write to the websocket")
		//	continue
		//}
	}
}

func WriteRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	for {
		ListenersMutex.RLock()

		if Listeners[callId] == nil {
			ListenersMutex.RUnlock()
			return
		}

		res, err := ws.ReadFromWebSocket(Listeners[callId].Ws)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the websocket")
			ListenersMutex.RUnlock()
			continue
		}

		log.Info().Str("res", string(res)).Msg("received payload from the websocket")
		fmt.Printf("received payload from the websocket, si-sp: %s:%d, payload: \n%s\n", Listeners[callId].RemoteAddr.IP.String(), Listeners[callId].RemoteAddr.Port, string(res))

		//TODO: send this text from cartesia to LLM, then back to cartesia for text to speech
		//resBuffer := make([]byte, 1024)
		//resBuffer := parseRtp(string(reqBuffer))

		//fmt.Printf("\n\npayload to be written to udp the socket, payload: \n%s\n", string(resBuffer))

		//_, err := Listeners[callId].Udp.WriteToUDP(resBuffer, Listeners[callId].RemoteAddr)
		//if err != nil {
		//log.Error().Err(err).Msgf("failed to write to the udp socket")
		//continue
		//}

		ListenersMutex.RUnlock()
	}
}
