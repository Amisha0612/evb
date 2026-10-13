package logger

import (
	"io"
	"sync"
)

func NewMockLogger(output io.Writer) *logger {
	return &logger{
		out:   output,
		level: getLevel("DEBUG"),
		mutex: &sync.RWMutex{},
	}
}

func NewMockLoggerWithCorrID(output io.Writer, correlationID string) *logger {
	return &logger{
		out:   output,
		level: getLevel("DEBUG"),
		mutex: &sync.RWMutex{},
	}
}
