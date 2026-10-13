package logger

import (
	"bytes"
	"strings"
)

type level int

var (
	fatal    = "FATAL"
	warn     = "WARN"
	debug    = "DEBUG"
	errorLog = "ERROR"
)

// use *Level suffix for constants
const (
	FatalLevel level = iota + 1
	ErrorLevel
	WarnLevel
	InfoLevel
	DebugLevel
)

func (l level) String() string {
	switch l {
	case FatalLevel:
		return "FATAL"
	case ErrorLevel:
		return "ERROR"
	case WarnLevel:
		return "WARN"
	case DebugLevel:
		return "DEBUG"
	default:
		return "INFO"
	}
}

const (
	redColor    = 31
	yellowColor = 33
	blueColor   = 36
	normalColor = 37
)

func (l level) colorCode() int {
	switch l {
	case FatalLevel, ErrorLevel:
		return redColor
	case WarnLevel:
		return yellowColor
	case InfoLevel:
		return blueColor
	case DebugLevel:
		return normalColor
	default:
		return normalColor
	}
}

func (l level) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(l.String())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func getLevel(l string) level {
	switch strings.ToUpper(l) {
	case fatal:
		return FatalLevel
	case errorLog:
		return ErrorLevel
	case warn:
		return WarnLevel
	case debug:
		return DebugLevel
	default:
		return InfoLevel
	}
}
