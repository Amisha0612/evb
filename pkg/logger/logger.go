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

type Logger struct {
	out    io.Writer
	level  level
	mutex  *sync.RWMutex
	System map[string]interface{} `json:"system"`
}

func NewLogger() *Logger {
	return &Logger{
		out:   os.Stdout,
		level: InfoLevel, // ✅ use InfoLevel constant
		mutex: &sync.RWMutex{},
	}
}

func (k *Logger) emit(lvl level, output func() string) {
	k.mutex.RLock()
	currentLevel := k.level
	k.mutex.RUnlock()

	if lvl > currentLevel {
		return
	}

	k.System = fetchSystemStats()

	msg := fmt.Sprintf("\033[%dm[%s] %s\033[0m\n", lvl.colorCode(), lvl.String(), output())
	_, _ = fmt.Fprint(k.out, msg)
}

func (k *Logger) log(lvl level, args ...interface{}) {
	k.emit(lvl, func() string { return fmt.Sprint(args...) })
}

func (k *Logger) logf(lvl level, format string, args ...interface{}) {
	k.emit(lvl, func() string { return fmt.Sprintf(format, args...) })
}

func Log(args ...interface{}) {
	defaultLogger.log(InfoLevel, args...)
}

func Logf(format string, args ...interface{}) {
	defaultLogger.logf(InfoLevel, format, args...)
}

func Info(args ...interface{}) {
	defaultLogger.log(InfoLevel, args...)
}

func Infof(format string, args ...interface{}) {
	defaultLogger.logf(InfoLevel, format, args...)
}

func Print(args ...interface{}) {
	defaultLogger.log(InfoLevel, args...)
}

func Printf(format string, args ...interface{}) {
	defaultLogger.logf(InfoLevel, format, args...)
}

func Debug(args ...interface{}) {
	defaultLogger.log(DebugLevel, args...)
}

func Debugf(format string, args ...interface{}) {
	defaultLogger.logf(DebugLevel, format, args...)
}

func Warn(args ...interface{}) {
	defaultLogger.log(WarnLevel, args...)
}

func Warnf(format string, args ...interface{}) {
	defaultLogger.logf(WarnLevel, format, args...)
}

func Error(args ...interface{}) {
	defaultLogger.log(ErrorLevel, args...)
}

func Errorf(format string, args ...interface{}) {
	defaultLogger.logf(ErrorLevel, format, args...)
}

func Fatal(args ...interface{}) {
	defaultLogger.log(FatalLevel, args...)
	os.Exit(1)
}

func Fatalf(format string, args ...interface{}) {
	defaultLogger.logf(FatalLevel, format, args...)
	os.Exit(1)
}

func Update(level string) {
	defaultLogger.Update(level)
}

func (k *Logger) Update(level string) {
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

func (k *Logger) Level() string {
	k.mutex.RLock()
	defer k.mutex.RUnlock()
	return k.level.String()
}
