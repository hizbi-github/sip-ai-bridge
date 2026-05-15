package rtp

import (
	"errors"
	"fmt"
	"net"
	"sip-ai-bridge/ws"
	"strconv"

	"github.com/coder/websocket"
	"github.com/rs/zerolog/log"
)

var Listeners map[string]*RtpListener

type RtpListener struct {
	RemoteAddr *net.UDPAddr
	LocalAddr  *net.UDPAddr
	Udp        *net.UDPConn
	Ws         *websocket.Conn
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

func DeleteRtpListener(callId string) error {
	if Listeners[callId] == nil {
		//if _, ok := Listeners[callId]; !ok {
		return errors.New("no rtp listener found for the call-id")
	}

	Listeners[callId].Udp.Close()
	ws.DeleteWebSocket(Listeners[callId].Ws)

	Listeners[callId] = nil
	return nil
}

func ReadRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	for {
		if Listeners[callId] == nil {
			//if _, ok := Listeners[callId]; !ok {
			log.Info().Msgf("udp socket closed, stopping read")
			return
		}

		resBuffer := make([]byte, 1024)
		_, remoteAddr, err := Listeners[callId].Udp.ReadFromUDP(resBuffer)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the udp socket")
			continue
		}

		//TODO: checkout rtp packet header/payload format to extract PCMU/PCMA audio data
		//if !strings.HasPrefix(string(reqBuffer), "rtp signature") {
		//continue
		//}

		//log.Debug().Msgf("received payload from udp the socket, payload: %s", string(reqBuffer))
		fmt.Printf("received payload from the udp socket, si-sp: %s:%d, payload: \n%s\n", remoteAddr.IP.String(), remoteAddr.Port, string(resBuffer))

		err = ws.WriteToWebSocket(Listeners[callId].Ws, resBuffer)
		if err != nil {
			log.Error().Err(err).Msgf("failed to write to the websocket")
			continue
		}
	}
}

func WriteRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	for {
		if Listeners[callId] == nil {
			return
		}

		res, err := ws.ReadFromWebSocket(Listeners[callId].Ws)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the websocket")
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
	}
}
