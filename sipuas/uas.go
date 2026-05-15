package sipuas

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sip-ai-bridge/utils"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func CreateUdpSocket() {
	udpAddr, err := net.ResolveUDPAddr("udp", ":5060")
	if err != nil {
		log.Error().Err(err).Msgf("failed to resolve the udp socket address")
		os.Exit(1)
	}

	udpSocket, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Error().Err(err).Msgf("failed to listen on the udp socket")
		os.Exit(1)
	}
	defer udpSocket.Close()

	for {
		reqBuffer := make([]byte, 1024)
		_, remoteAddr, err := udpSocket.ReadFromUDP(reqBuffer)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read from the udp socket")
			continue
		}

		if !strings.HasPrefix(string(reqBuffer), "INVITE") && !strings.HasPrefix(string(reqBuffer), "BYE") {
			continue
		}

		//log.Debug().Msgf("received payload from udp the socket, payload: %s", string(reqBuffer))
		fmt.Printf("received payload from udp the socket, payload: \n%s\n", string(reqBuffer))

		resBuffer := []byte{}
		if strings.HasPrefix(string(reqBuffer), "INVITE") {
			resBuffer, err = generate200Res(string(reqBuffer))
			if err != nil {
				log.Error().Err(err).Msgf("failed to generate 200-OK response")
				continue
			}
		} else if strings.HasPrefix(string(reqBuffer), "BYE") {
			resBuffer, err = generateByeRes(string(reqBuffer))
			if err != nil {
				log.Error().Err(err).Msgf("failed to generate 200-OK response")
				continue
			}
		}

		fmt.Printf("\n\npayload to be written to udp the socket, payload: \n%s\n", string(resBuffer))

		_, err = udpSocket.WriteToUDP(resBuffer, remoteAddr)
		if err != nil {
			log.Error().Err(err).Msgf("failed to write to the udp socket")
			continue
		}
	}
}

func generate200Res(inviteReq string) ([]byte, error) {
	callId := ""
	answerRes := "SIP/2.0 200 OK\r\n"

	for _, line := range strings.Split(inviteReq, "\r\n") {
		//hdrKey := strings.TrimSpace(strings.Split(line, ":")[0])
		//hdrValue := strings.TrimSpace(strings.Split(line, ":")[1])

		if strings.Contains(line, "Via:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "From:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "To:") {
			answerRes = answerRes + strings.TrimSpace(line) + ";tag=" + uuid.NewString() + "\r\n"
		}

		if strings.Contains(line, "Call-ID:") {
			callId = strings.Split(line, "Call-ID: ")[1]
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "CSeq:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}
	}

	answerRes = answerRes + "Contact: <sip:127.0.0.1:5060>\r\n"

	sdpOffer := strings.Split(inviteReq, "\r\n\r\n")[1]
	sdpAnswer, err := generateSdpAnswer(callId, sdpOffer)
	if err != nil {
		return nil, err
	}

	answerRes = answerRes + "Content-Type: application/sdp\r\n"
	answerRes = answerRes + "Content-Length: " + strconv.Itoa(len(sdpAnswer)-40) + "\r\n\r\n"
	answerRes = answerRes + string(sdpAnswer)

	return []byte(answerRes), nil
}

func generateSdpAnswer(callId, sdpOffer string) ([]byte, error) {
	remoteIp := strings.Split(strings.Split(sdpOffer, "c=IN IP4 ")[1], "\r\n")[0]
	remotePort := strings.Split(strings.Split(sdpOffer, "m=audio ")[1], " RTP/AVP")[0]

	req := utils.HttpRequest{
		Method:  "GET",
		Url:     fmt.Sprintf("http://127.0.0.1:8080/create-rtp-listener?call-id=%s&remote-ip=%s&remote-port=%s", callId, remoteIp, remotePort),
		Headers: nil,
		Body:    nil,
	}

	res, err := utils.NewHttpReq(req)
	if err != nil {
		return nil, err
	}

	localIp := res.Headers.Get("local-ip")
	localPort := res.Headers.Get("local-port")

	sdpAnswer := ""
	sdpAnswer = sdpAnswer + "v=0\r\n"
	sdpAnswer = sdpAnswer + "o=- " + strconv.FormatInt(time.Now().Unix(), 10) + " " + strconv.FormatInt(time.Now().Unix(), 10) + " IN IP4 " + localIp + "\r\n"
	sdpAnswer = sdpAnswer + "s=-\r\n"
	sdpAnswer = sdpAnswer + "c=IN IP4 " + localIp + "\r\n"
	sdpAnswer = sdpAnswer + "t=0 0\r\n"
	sdpAnswer = sdpAnswer + "m=audio " + localPort + " RTP/AVP 0 101\r\n"
	sdpAnswer = sdpAnswer + "a=sendrecv\r\n"
	sdpAnswer = sdpAnswer + "a=rtpmap:0 PCMU/8000\r\n"
	sdpAnswer = sdpAnswer + "a=rtpmap:101 telephone-event/8000\r\n"
	sdpAnswer = sdpAnswer + "a=fmtp:101 0-16\r\n"

	return []byte(sdpAnswer), nil
}

func generateByeRes(inviteReq string) ([]byte, error) {
	callId := ""
	answerRes := "SIP/2.0 200 OK\r\n"

	for _, line := range strings.Split(inviteReq, "\r\n") {
		//hdrKey := strings.TrimSpace(strings.Split(line, ":")[0])
		//hdrValue := strings.TrimSpace(strings.Split(line, ":")[1])

		if strings.Contains(line, "Via:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "From:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "To:") {
			answerRes = answerRes + strings.TrimSpace(line) + ";tag=" + uuid.NewString() + "\r\n"
		}

		if strings.Contains(line, "Call-ID:") {
			callId = strings.Split(line, "Call-ID: ")[1]
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}

		if strings.Contains(line, "CSeq:") {
			answerRes = answerRes + strings.TrimSpace(line) + "\r\n"
		}
	}

	answerRes = answerRes + "Contact: <sip:127.0.0.1:5060>\r\n"
	answerRes = answerRes + "Content-Length: 0\r\n\r\n"

	req := utils.HttpRequest{
		Method:  "GET",
		Url:     fmt.Sprintf("http://127.0.0.1:8080/delete-rtp-listener?call-id=%s", callId),
		Headers: nil,
		Body:    nil,
	}

	res, err := utils.NewHttpReq(req)
	if err != nil {
		return nil, err
	}

	if res.Code != 200 {
		return nil, errors.New(string(res.Body))
	}

	return []byte(answerRes), nil
}
