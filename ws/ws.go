package ws

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

//var ws *websocket.Conn

func CreateWebSocket() (*websocket.Conn, error) {
	authHdrs := http.Header{}
	authHdrs.Add("X-API-Key", "dummy")
	//TODO: read API key from ENVs

	url := "wss://api.cartesia.ai/stt/websocket"
	url = url + "?model=ink-whisper"
	url = url + "&language=en"
	url = url + "&encoding=pcm_mulaw"
	url = url + "&sample_rate=8000"
	url = url + "&min_volume=0.0"
	url = url + "&max_silence_duration_secs=250"
	url = url + "&cartesia_version=2026-03-01"

	var err error
	ws, _, err := websocket.Dial(context.TODO(), url, &websocket.DialOptions{
		HTTPHeader: authHdrs,
	})
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
	resType, res, err := ws.Read(context.TODO())
	if err != nil {
		return nil, err
	}

	if resType != websocket.MessageText {
		return nil, nil
	}

	return res, nil
}

func WriteToWebSocket(ws *websocket.Conn, rawAudioPacket []byte) error {
	err := ws.Write(context.TODO(), websocket.MessageBinary, rawAudioPacket)
	if err != nil {
		return err
	}

	return nil
}
