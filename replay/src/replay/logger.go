package replay

import (
	"log"
	"os"
)

// Logger provides a way to send different types of log messages to stderr/stdout
type Logger struct {
	stderr  *log.Logger
	stdout  *log.Logger
	toClose []closeable
}

type closeable interface {
	Close() error
}

// NewLogger creates a new logger
func NewLogger(stdout string, stderr string) (logger *Logger, err error) {
	var (
		stderrWriter = os.Stderr
		stdoutWriter = os.Stdout
		toClose      []closeable
	)

	if stderr != "" {
		if stderrWriter, err = os.OpenFile(stderr, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
			return
		}
		toClose = append(toClose, stderrWriter)
	}
	if stdout != "" {
		if stdoutWriter, err = os.OpenFile(stdout, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
			return
		}
		toClose = append(toClose, stderrWriter)
	}

	logger = &Logger{
		stderr:  log.New(stderrWriter, "INFO ", log.LstdFlags|log.Lshortfile),
		stdout:  log.New(stdoutWriter, "ERROR ", log.LstdFlags|log.Lshortfile),
		toClose: toClose,
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

// Close the underlying files
func (l *Logger) Close() {
	for _, c := range l.toClose {
		c.Close()
	}
}
