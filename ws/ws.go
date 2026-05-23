package ws

import (
	"context"
	"io"
	"net/http"

	"github.com/coder/websocket"
	"github.com/rs/zerolog/log"
)

//var ws *websocket.Conn

func CreateSttWebSocket() (*websocket.Conn, error) {
	authHdrs := http.Header{}
	authHdrs.Add("X-API-Key", "dummy")
	//TODO: read API key from ENVs

	url := "wss://api.cartesia.ai/stt/websocket"
	//url := "wss://api.cartesia.ai/stt/turns/websocket"
	url = url + "?model=ink-2"
	url = url + "&language=en"
	url = url + "&encoding=pcm_mulaw"
	url = url + "&sample_rate=8000"
	//url = url + "&min_volume=0.0"
	//url = url + "&max_silence_duration_secs=250"
	url = url + "&cartesia_version=2026-03-01"

	var err error
	ws, res, err := websocket.Dial(context.TODO(), url, &websocket.DialOptions{
		HTTPHeader: authHdrs,
	})

	if res.Body != nil {
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		log.Debug().Str("res", string(resBody)).Msgf("received payload from the stt websocket")
	}

	if err != nil {
		return nil, err
	}

	return ws, nil
}

func CreateTtsWebSocket() (*websocket.Conn, error) {
	authHdrs := http.Header{}
	authHdrs.Add("X-API-Key", "dummy")
	//TODO: read API key from ENVs

	url := "wss://api.cartesia.ai/tts/websocket"
	//url := "wss://api.cartesia.ai/stt/turns/websocket"
	//url = url + "?model=sonic-3.5"
	//url = url + "?model=sonic-latest"
	//url = url + "&language=en"
	//url = url + "&encoding=pcm_mulaw"
	//url = url + "&sample_rate=8000"
	//url = url + "&max_buffer_delay_ms=500"
	url = url + "?cartesia_version=2026-03-01"

	var err error
	ws, res, err := websocket.Dial(context.TODO(), url, &websocket.DialOptions{
		HTTPHeader: authHdrs,
	})

	if res.Body != nil {
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		log.Debug().Str("res", string(resBody)).Msgf("received payload from the tts websocket")
	}

	if err != nil {
		return nil, err
	}

	return ws, nil
}

func DeleteWebSocket(ws *websocket.Conn) error {
	err := ws.Close(websocket.StatusNormalClosure, "OK")
	if err != nil {
		return err
	}

	return nil
}

func ReadFromWebSocket(ws *websocket.Conn) ([]byte, error) {
	//resType, res, err := ws.Read(context.TODO())
	_, res, err := ws.Read(context.TODO())
	if err != nil {
		return nil, err
	}

	//TODO: check if this is somehow blocking interim responses
	//if resType != websocket.MessageText {
	//return nil, nil
	//}

	return res, nil
}

func WriteToWebSocket(ws *websocket.Conn, rawAudioPacket []byte) error {
	err := ws.Write(context.TODO(), websocket.MessageBinary, rawAudioPacket)
	if err != nil {
		return err
	}

	return nil
}
