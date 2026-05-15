package utils

import (
	"errors"
	"io"
	"net/http"
)

type HttpRequest struct {
	Method  string
	Url     string
	Headers map[string][]string
	Body    io.Reader
}

type HttpResponse struct {
	Code    int
	Headers http.Header
	Body    []byte
}

func NewHttpReq(req HttpRequest) (*HttpResponse, error) {
	if req.Method == "" || req.Url == "" {
		return nil, errors.New("[NewHttpReq] either req.Method or req.Url is empty")
	}

	newReq, err := http.NewRequest(req.Method, req.Url, req.Body)
	if err != nil {
		return nil, err
	}

	if req.Headers != nil {
		newReq.Header = req.Headers
	}

	res, err := http.DefaultClient.Do(newReq)
	if err != nil {
		return nil, err
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return &HttpResponse{
		Code:    res.StatusCode,
		Headers: res.Header.Clone(),
		Body:    resBody,
	}, nil
}
