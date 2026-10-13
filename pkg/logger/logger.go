package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	defaultLogger = NewLogger()
)

type logger struct {
	out    io.Writer
	level  level
	mutex  *sync.RWMutex
	System map[string]interface{} `json:"system"`
}

func NewLogger() *logger {
	return &logger{
		out:   os.Stdout,
		level: InfoLevel, // ✅ use InfoLevel constant
		mutex: &sync.RWMutex{},
	}
}

func (k *logger) logger(lvl level, format string, args ...interface{}) {
	k.mutex.RLock()
	currentLevel := k.level
	k.mutex.RUnlock()

	if lvl > currentLevel {
		return
	}

	k.System = fetchSystemStats()

	var output string
	if format == "" {
		output = fmt.Sprint(args...)
	} else {
		output = fmt.Sprintf(format, args...)
	}

	msg := fmt.Sprintf("\033[%dm[%s] %s\033[0m\n", lvl.colorCode(), lvl.String(), output)
	fmt.Fprint(k.out, msg)
}

func Log(args ...interface{}) {
	defaultLogger.logger(InfoLevel, "", args...)
}

func Logf(format string, args ...interface{}) {
	defaultLogger.logger(InfoLevel, format, args...)
}

func Info(args ...interface{}) {
	defaultLogger.logger(InfoLevel, "", args...)
}

func Infof(format string, args ...interface{}) {
	defaultLogger.logger(InfoLevel, format, args...)
}

func Print(args ...interface{}) {
	defaultLogger.logger(InfoLevel, "", args...)
}

func Printf(format string, args ...interface{}) {
	defaultLogger.logger(InfoLevel, format, args...)
}

func Debug(args ...interface{}) {
	defaultLogger.logger(DebugLevel, "", args...)
}

func Debugf(format string, args ...interface{}) {
	defaultLogger.logger(DebugLevel, format, args...)
}

func Warn(args ...interface{}) {
	defaultLogger.logger(WarnLevel, "", args...)
}

func Warnf(format string, args ...interface{}) {
	defaultLogger.logger(WarnLevel, format, args...)
}

func Error(args ...interface{}) {
	defaultLogger.logger(ErrorLevel, "", args...)
}

func Errorf(format string, args ...interface{}) {
	defaultLogger.logger(ErrorLevel, format, args...)
}

func Fatal(args ...interface{}) {
	defaultLogger.logger(FatalLevel, "", args...)
	os.Exit(1)
}

func Fatalf(format string, args ...interface{}) {
	defaultLogger.logger(FatalLevel, format, args...)
	os.Exit(1)
}

func Update(level string) {
	defaultLogger.Update(level)
}

func (k *logger) Update(level string) {
	k.mutex.RLock()
	currentLevel := k.level
	k.mutex.RUnlock()

	newLevel := getLevel(level)
	if newLevel != currentLevel {
		Debugf("Changing log level from %s to %s due to override service", currentLevel, newLevel)

		k.mutex.Lock()
		k.level = newLevel
		k.mutex.Unlock()
	}
}

func (k *logger) Level() string {
	k.mutex.RLock()
	defer k.mutex.RUnlock()
	return k.level.String()
}
