package rtp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
	RemoteAddr      *net.UDPAddr
	LocalAddr       *net.UDPAddr
	Udp             *net.UDPConn
	SttWs           *websocket.Conn
	TtsWs           *websocket.Conn
	AudioChunkRead  []byte
	AudioChunkWrite []byte
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
	sttWs, err := ws.CreateSttWebSocket()
	if err != nil {
		return nil, err
	}

	ttsWs, err := ws.CreateTtsWebSocket()
	if err != nil {
		return nil, err
	}

	rtpListener := RtpListener{
		RemoteAddr: remoteAddr,
		LocalAddr:  localAddr,
		Udp:        udpSocket,
		SttWs:      sttWs,
		TtsWs:      ttsWs,
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
	ws.DeleteWebSocket(Listeners[callId].SttWs)
	ws.DeleteWebSocket(Listeners[callId].TtsWs)

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

		//TODO: try to run below in a goroutine
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

		listener.AudioChunkRead = append(listener.AudioChunkRead, resBuffer...)

		if len(listener.AudioChunkRead) >= 800 {
			err = ws.WriteToWebSocket(listener.SttWs, listener.AudioChunkRead)
			if err != nil {
				log.Error().Err(err).Msgf("failed to write to the websocket")
				continue
			}

			ListenersMutex.Lock()
			Listeners[callId].AudioChunkRead = []byte{}
			ListenersMutex.Unlock()
		}

		//TODO: might not need mutex locking here at all
		ListenersMutex.Lock()
		Listeners[callId].AudioChunkRead = append(listener.AudioChunkRead, resBuffer...)
		ListenersMutex.Unlock()
	}
}

func ReadStt(callId string) {
	defer ws.DeleteWebSocket(Listeners[callId].SttWs)

	for {
		ListenersMutex.RLock()
		listener := Listeners[callId]
		ListenersMutex.RUnlock()

		if listener == nil {
			return
		}

		res, err := ws.ReadFromWebSocket(listener.SttWs)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the stt websocket")
			return
		}

		go func(wsRes []byte, ttsWs *websocket.Conn) {
			type CartesiaSttResponse struct {
				IsFinal bool   `json:"is_final"`
				Text    string `json:"text"`
			}

			sttRes := CartesiaSttResponse{}
			err = json.Unmarshal(wsRes, &sttRes)
			if err != nil {
				log.Error().Err(err).Msgf("failed to unmarshal the response from the stt websocket")
				return
			}

			log.Info().Bool("final", sttRes.IsFinal).Str("text", sttRes.Text).Msg("received payload from the stt websocket")

			ttsReq := map[string]any{
				"model_id":   "sonic-3.5",
				"transcript": sttRes.Text,
				"voice": map[string]string{
					"mode": "id",
					"id":   "a0e99841-438c-4a64-b679-ae501e7d6091",
				},
				"language":   "en",
				"context_id": "ab977222-f9e0-4563-a1c0-5a934ae8fdd6",
				//"context_id": uuid.NewString(),
				"output_format": map[string]any{
					"container":   "raw",
					"encoding":    "pcm_mulaw",
					"sample_rate": 8000,
				},
				"continue": false,
			}

			req, err := json.Marshal(ttsReq)
			if err != nil {
				log.Error().Err(err).Msgf("failed to marshal the request for the tts websocket")
				return
			}

			err = ws.WriteToWebSocket(ttsWs, req)
			if err != nil {
				log.Error().Err(err).Msgf("failed to write to the tts websocket")
				return
			}
		}(res, listener.TtsWs)
	}
}

func ReadTts(callId string) {
	defer ws.DeleteWebSocket(Listeners[callId].TtsWs)

	for {
		ListenersMutex.RLock()
		listener := Listeners[callId]
		ListenersMutex.RUnlock()

		if listener == nil {
			return
		}

		res, err := ws.ReadFromWebSocket(listener.TtsWs)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the tts websocket")
			return
		}

		go func(wsRes []byte, udpConn *net.UDPConn) {
			type CartesiaTtsResponse struct {
				Type      string `json:"type"`
				ContextID string `json:"context_id"`
				Done      bool   `json:"done"`
				Data      string `json:"data"`
			}

			ttsRes := CartesiaTtsResponse{}
			err = json.Unmarshal(wsRes, &ttsRes)
			if err != nil {
				log.Error().Err(err).Msgf("failed to unmarshal the response from the tts websocket")
				return
			}

			if ttsRes.Type != "chunk" {
				return
			}

			ttsAudioChunkDecoded, err := base64.StdEncoding.DecodeString(ttsRes.Data)
			if err != nil {
				log.Error().Err(err).Msgf("failed to decode the base64 audio chunk from the tts websocket")
				return
			}

			log.Info().Bool("final", ttsRes.Done).Int("len", len(ttsAudioChunkDecoded)).Bytes("audio", ttsAudioChunkDecoded).Msg("received payload from the tts websocket")
			//log.Info().Str("res", string(wsRes)).Msg("received payload from the tts websocket")

			resBuffer := make([]byte, 172)
			resBuffer = ttsAudioChunkDecoded

			//TODO: divide the tts audio chunk into 160 bytes and send those mini-chunks one by one
			_, err = udpConn.WriteToUDP(resBuffer, Listeners[callId].RemoteAddr)
			if err != nil {
				log.Error().Err(err).Msgf("failed to write to the udp socket")
				return
			}
		}(res, listener.Udp)
	}
}
