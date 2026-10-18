package relay

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/Amisha0612/evb/internal/models"
)

const defaultDeadLetterPath = "dead-letter.jsonl"

// deadLetter is one line of the dead-letter file: an event that a sink did not
// accept, with enough context to replay it.
type deadLetter struct {
	FailedAt time.Time             `json:"failedAt"`
	Server   string                `json:"server"`
	SinkType string                `json:"sinkType"`
	SinkID   string                `json:"sinkId"`
	Attempts int                   `json:"attempts"`
	Error    string                `json:"error"`
	Event    *models.StitchedEvent `json:"event"`
}

// deadLetterStore appends failed deliveries to a JSON Lines file. An empty path
// disables it. The file is created on the first failure.
type deadLetterStore struct {
	path string
	mu   sync.Mutex
}

func newDeadLetter(path string) *deadLetterStore {
	return &deadLetterStore{path: path}
}

// Write records a failed delivery. It returns an error only when the record could
// not be stored, in which case the event is lost and the caller must say so.
func (d *deadLetterStore) Write(sinkType, sinkID string, event *models.StitchedEvent, attempts int, cause error) error {
	if d == nil || d.path == "" {
		return nil
	}

	rec := deadLetter{
		FailedAt: time.Now().UTC(),
		SinkType: sinkType,
		SinkID:   sinkID,
		Attempts: attempts,
		Event:    event,
	}

	if event != nil {
		rec.Server = event.Server
	}

	if cause != nil {
		rec.Error = cause.Error()
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	line = append(line, '\n')

	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	if _, err := f.Write(line); err != nil {
		_ = f.Close()

		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return err
	}

	return f.Close()
}

// deadLetterPathFromEnv returns DEAD_LETTER_PATH, or the default file name when it
// is unset. Set it to "off" to disable the dead-letter file.
func deadLetterPathFromEnv() string {
	v := os.Getenv("DEAD_LETTER_PATH")

	switch v {
	case "":
		return defaultDeadLetterPath
	case "off":
		return ""
	default:
		return v
	}
}
