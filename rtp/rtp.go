package rtp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net"
	"sip-ai-bridge/ws"
	"slices"
	"strconv"
	"sync"

	"github.com/coder/websocket"
	"github.com/rs/zerolog/log"
)

var Listeners map[string]*RtpListener
var ListenersMutex sync.RWMutex
var seqCounter uint16
var seqTimestampCounter uint32

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
	//remoteAddr, err := net.ResolveUDPAddr("udp", remoteIp+":"+remotePort)
	remoteAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+remotePort)
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

		if len(listener.AudioChunkRead) >= 1600 {
			go func(wsConn *websocket.Conn, chunks []byte) {
				err = ws.WriteToWebSocket(wsConn, chunks)
				if err != nil {
					log.Error().Err(err).Msgf("failed to write to the websocket")
					return
					//continue
				}
			}(listener.SttWs, listener.AudioChunkRead)

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
					"id":   "f786b574-daa5-4673-aa0c-cbe3e8534c02",
					//"id":   "a5136bf9-224c-4d76-b823-52bd5efcffcc",
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
			//return
			continue
		}

		//go func(wsRes []byte, udpConn *net.UDPConn, remoteAddr *net.UDPAddr) {
		type CartesiaTtsResponse struct {
			Type      string `json:"type"`
			ContextID string `json:"context_id"`
			Done      bool   `json:"done"`
			Data      string `json:"data"`
		}

		ttsRes := CartesiaTtsResponse{}
		//err = json.Unmarshal(wsRes, &ttsRes)
		err = json.Unmarshal(res, &ttsRes)
		if err != nil {
			log.Error().Err(err).Msgf("failed to unmarshal the response from the tts websocket")
			//return
			continue
		}

		if ttsRes.Type != "chunk" {
			//return
			continue
		}

		audioSeq, err := base64.StdEncoding.DecodeString(ttsRes.Data)
		if err != nil {
			log.Error().Err(err).Msgf("failed to decode the base64 audio chunk from the tts websocket")
			//return
			continue
		}

		log.Info().Bool("final", ttsRes.Done).Int("len", len(audioSeq)).Float64("chunks", math.Ceil(float64(len(audioSeq))/160)).Msg("received payload from the tts websocket")
		//log.Info().Str("res", string(wsRes)).Msg("received payload from the tts websocket")

		for chunk := range slices.Chunk(audioSeq, 160) {
			//rtpHeader := make([]byte, 12)
			rtpHeader := []byte{}
			rtpHeader = append(rtpHeader, byte(0b10000000), byte(0b00000000))

			seqNumber := seqCounter
			rtpHeader = append(rtpHeader, uint8(seqNumber>>8), uint8(seqNumber&0b11111111))
			seqCounter = seqCounter + 1

			seqTimestamp := seqTimestampCounter
			rtpHeader = append(rtpHeader, uint8(seqTimestamp>>24))
			rtpHeader = append(rtpHeader, uint8(seqTimestamp>>16))
			rtpHeader = append(rtpHeader, uint8(seqTimestamp>>8))
			rtpHeader = append(rtpHeader, uint8(seqTimestamp&0b11111111))
			seqTimestampCounter = seqTimestampCounter + 160

			syncId := uint32(123456789)
			rtpHeader = append(rtpHeader, uint8(syncId>>24))
			rtpHeader = append(rtpHeader, uint8(syncId>>16))
			rtpHeader = append(rtpHeader, uint8(syncId>>8))
			rtpHeader = append(rtpHeader, uint8(syncId&0b11111111))

			rtpPacket := append(rtpHeader, chunk...)

			//_, err = udpConn.WriteToUDP(rtpPacket, remoteAddr)
			_, err = listener.Udp.WriteToUDP(rtpPacket, listener.RemoteAddr)
			if err != nil {
				log.Error().Err(err).Msgf("failed to write to the udp socket")
				continue
			}

			log.Info().
				//Str("remote-ip", remoteAddr.IP.String()).
				//Int("remote-port", remoteAddr.Port).
				Str("remote-ip", listener.RemoteAddr.IP.String()).
				Int("remote-port", listener.RemoteAddr.Port).
				Int("rtp-header-len", len(rtpHeader)).
				Int("rtp-body-len", len(chunk)).
				Int("rtp-packet-len", len(rtpPacket)).
				Msg("wrote rtp payload to the udp socket")

			//fmt.Printf("rtp packet raw: %+v\n", rtpPacket)

			//time.Sleep(20 * time.Millisecond)
		}
		//}(res, listener.Udp, listener.RemoteAddr)
	}
}
