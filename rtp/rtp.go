package rtp

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/rs/zerolog/log"
)

var Listeners map[string]*RtpListener

type RtpListener struct {
	RemoteAddr *net.UDPAddr
	LocalAddr  *net.UDPAddr
	Udp        *net.UDPConn
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

	rtpListener := RtpListener{
		RemoteAddr: remoteAddr,
		LocalAddr:  localAddr,
		Udp:        udpSocket,
	}

	return &rtpListener, nil
}

func DeleteRtpListener(callId string) error {
	if Listeners[callId] == nil {
		//if _, ok := Listeners[callId]; !ok {
		return errors.New("no rtp listener found for the call-id")
	}

	Listeners[callId].Udp.Close()
	Listeners[callId] = nil

	return nil
}

func ReadRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	reqBuffer := make([]byte, 1024)
	for {
		if Listeners[callId] == nil {
			//if _, ok := Listeners[callId]; !ok {
			log.Info().Msgf("udp socket closed, stopping read")
			return
		}

		_, remoteAddr, err := Listeners[callId].Udp.ReadFromUDP(reqBuffer)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the udp socket")
			continue
		}

		//TODO: checkout rtp packet header/payload format to extract PCMU/PCMA audio data
		//if !strings.HasPrefix(string(reqBuffer), "rtp signature") {
		//continue
		//}

		//log.Debug().Msgf("received payload from udp the socket, payload: %s", string(reqBuffer))
		fmt.Printf("received payload from udp the socket, si-sp: %s:%d, payload: \n%s\n", remoteAddr.IP.String(), remoteAddr.Port, string(reqBuffer))
	}
}

func WriteRtp(callId string) {
	defer Listeners[callId].Udp.Close()

	for {
		if Listeners[callId] == nil {
			return
		}

		resBuffer := make([]byte, 1024)
		//resBuffer := parseRtp(string(reqBuffer))

		fmt.Printf("\n\npayload to be written to udp the socket, payload: \n%s\n", string(resBuffer))

		_, err := Listeners[callId].Udp.WriteToUDP(resBuffer, Listeners[callId].RemoteAddr)
		if err != nil {
			log.Error().Err(err).Msgf("failed to write to the udp socket")
			continue
		}
	}
}
