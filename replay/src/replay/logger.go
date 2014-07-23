package replay

import (
	"log"
	"os"
)

// Logger provides a way to send different types of log messages to stderr/stdout
type Logger struct {
	stderr *log.Logger
	stdout *log.Logger
}

func NewLogger(stdout string, stderr string) (logger *Logger, err error) {
	var (
		stderrWriter = os.Stderr
		stdoutWriter = os.Stdout
	)

	if stderr != "" {
		if stderrWriter, err = os.OpenFile(stderr, os.O_APPEND, os.ModeAppend); err != nil {
			return
		}
	}
	if stdout != "" {
		if stdoutWriter, err = os.OpenFile(stdout, os.O_APPEND, os.ModeAppend); err != nil {
			return
		}
	}

	logger = &Logger{
		log.New(stderrWriter, "", log.LstdFlags),
		log.New(stdoutWriter, "", log.LstdFlags),
	}
	return
}

// Info prints message to stdout
func (l *Logger) Info(v ...interface{}) {
	l.stdout.Print(v...)
}

// Infof prints message to stdout
func (l *Logger) Infof(format string, v ...interface{}) {
	l.stdout.Printf(format, v...)
}

// Error prints message to stderr
func (l *Logger) Error(v ...interface{}) {
	l.stderr.Print(v...)
}

// Errorf prints message to stderr
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.stderr.Printf(format, v...)
}
