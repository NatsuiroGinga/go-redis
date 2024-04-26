package parser

import (
	"bytes"
	"errors"

	"go-redis/interface/resp"
)

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (resp.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}
