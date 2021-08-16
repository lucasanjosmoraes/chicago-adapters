package amqpadapter

import (
	"strconv"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/publisher"
)

func hasAttemptsLeft(attempts int, hs []publisher.EventHeader) bool {
	if attempts <= 0 {
		return true
	}

	retryCounter := getCounter(hs)

	return retryCounter < attempts
}

func increaseAttemptsHeader(hs []publisher.EventHeader) []publisher.EventHeader {
	retryCounter := getCounter(hs)
	newCounter := retryCounter + 1

	hs = append(hs, publisher.EventHeader{
		Key:   requeueHeader,
		Value: []byte(strconv.Itoa(newCounter)),
	})

	return hs
}

func getCounter(hs []publisher.EventHeader) int {
	for _, h := range hs {
		if h.Key == requeueHeader {
			counterS := string(h.Value)
			counter, _ := strconv.Atoi(counterS)

			return counter
		}
	}

	return 0
}
