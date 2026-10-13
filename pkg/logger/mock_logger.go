package logger

import (
	"io"
	"sync"
)

func NewMockLogger(output io.Writer) *Logger {
	return &Logger{
		out:   output,
		level: getLevel("DEBUG"),
		mutex: &sync.RWMutex{},
	}
}

func NewMockLoggerWithCorrID(output io.Writer, _ string) *Logger {
	return &Logger{
		out:   output,
		level: getLevel("DEBUG"),
		mutex: &sync.RWMutex{},
	}
}
