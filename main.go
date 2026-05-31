package main

import (
	"fmt"
	"net/http"
	"sip-ai-bridge/rtp"
	"sip-ai-bridge/sip"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	//logLvl, err := zerolog.ParseLevel(os.Getenv("API_LOG_LEVEL"))
	logLvl, err := zerolog.ParseLevel("debug")
	if err != nil {
		log.Error().Err(err).Msg("failed to parse log level")
		logLvl = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(logLvl)

	log.Info().Msg("starting sip server...")
	go sip.CreateUdpSocket()

	rtp.InitUdpPortRange()

	rtp.Listeners = map[string]*rtp.RtpListener{}

	httpMux := http.NewServeMux()

	//TODO: move the handlers into separate functions
	httpMux.HandleFunc("GET /create-rtp-listener", func(w http.ResponseWriter, r *http.Request) {
		callId := r.URL.Query().Get("call-id")
		rtpRemoteIp := r.URL.Query().Get("remote-ip")
		rtpRemotePort := r.URL.Query().Get("remote-port")

		log.Info().Str("path", r.URL.Path).Str("si-sp", r.RemoteAddr).
			Str("call-id", callId).
			Str("remote-ip", rtpRemoteIp).
			Str("remote-port", rtpRemotePort).
			Msg("received http request")

		rtpListener, err := rtp.CreateRtpListener(rtpRemoteIp, rtpRemotePort)
		if err != nil {
			log.Info().Str("path", r.URL.Path).Str("si-sp", r.RemoteAddr).
				Str("call-id", callId).
				Str("remote-ip", rtpRemoteIp).
				Str("remote-port", rtpRemotePort).
				Err(err).
				Msg("failed to create a rtp listener")

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		rtp.Listeners[callId] = rtpListener
		go rtp.ReadRtp(callId)
		go rtp.ReadStt(callId)
		go rtp.ReadTts(callId)
		go rtp.WriteRtp(callId)

		rtpLocalIp := "127.0.0.1"
		rtpLocalPort := strconv.Itoa(rtp.Listeners[callId].LocalAddr.Port)

		log.Info().Str("path", r.URL.Path).Str("si-sp", r.RemoteAddr).
			Str("call-id", callId).
			Str("remote-ip", rtpRemoteIp).
			Str("remote-port", rtpRemotePort).
			Str("local-ip", rtpLocalIp).
			Str("local-port", rtpLocalPort).
			Msg("created rtp listener")

		w.Header().Add("local-ip", rtpLocalIp)
		w.Header().Add("local-port", rtpLocalPort)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	httpMux.HandleFunc("GET /delete-rtp-listener", func(w http.ResponseWriter, r *http.Request) {
		callId := r.URL.Query().Get("call-id")

		log.Info().Str("path", r.URL.Path).Str("si-sp", r.RemoteAddr).
			Str("call-id", callId).
			Msg("received http request")

		listener, err := rtp.DeleteRtpListener(callId)
		if err != nil {
			log.Error().Err(err).Msgf("failed to delete the rtp listener")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(err.Error()))
			return
		}

		rtpRemoteIp := listener.RemoteAddr.IP.String()
		rtpRemotePort := strconv.Itoa(listener.RemoteAddr.Port)
		rtpLocalIp := "127.0.0.1"
		rtpLocalPort := strconv.Itoa(listener.LocalAddr.Port)

		log.Info().Str("path", r.URL.Path).Str("si-sp", r.RemoteAddr).
			Str("call-id", callId).
			Str("remote-ip", rtpRemoteIp).
			Str("remote-port", rtpRemotePort).
			Str("local-ip", rtpLocalIp).
			Str("local-port", rtpLocalPort).
			Msg("deleted rtp listener")

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", "8080"),
		Handler:      httpMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Info().Msg("starting http server...")
	if err := server.ListenAndServe(); err != nil {
		log.Error().Err(err).Msg("failed to start to http server")
	}
}

//curl 'http://127.0.0.1:8080/start-rtp-coms?in-ip=1.1.1.1&in-port=42433'
