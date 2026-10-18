package relay

import (
	"errors"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/Amisha0612/evb/pkg/logger"
)

const (
	defaultMaxAttempts = 3
	defaultRetryBase   = 200 * time.Millisecond
	defaultRetryMax    = 5 * time.Second
)

// retryPolicy describes how often and how fast a failed delivery is retried.
// The wait before retry n is a random duration in [0, min(Max, Base*2^(n-1))]
// ("full jitter"), so sinks that fail together do not retry in lockstep.
type retryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
}

// ceiling is the longest wait allowed after the given (1-based) failed attempt.
func (p retryPolicy) ceiling(attempt int) time.Duration {
	d := p.Base

	for i := 1; i < attempt; i++ {
		if d >= p.Max/2 {
			return p.Max
		}

		d *= 2
	}

	return min(d, p.Max)
}

// delay picks the actual wait after the given failed attempt.
func (p retryPolicy) delay(attempt int) time.Duration {
	c := p.ceiling(attempt)
	if c <= 0 {
		return 0
	}

	return time.Duration(rand.Int64N(int64(c) + 1))
}

// permanentError marks a failure that retrying cannot fix, such as a request the
// sink rejected as malformed.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func permanent(err error) error {
	if err == nil {
		return nil
	}

	return permanentError{err: err}
}

func isPermanent(err error) bool {
	var p permanentError

	return errors.As(err, &p)
}

// retryPolicyFromEnv reads RELAY_MAX_ATTEMPTS, RELAY_RETRY_BASE and RELAY_RETRY_MAX.
// Invalid values are logged and replaced by the defaults.
func retryPolicyFromEnv() retryPolicy {
	return retryPolicy{
		MaxAttempts: envInt("RELAY_MAX_ATTEMPTS", defaultMaxAttempts),
		Base:        envDuration("RELAY_RETRY_BASE", defaultRetryBase),
		Max:         envDuration("RELAY_RETRY_MAX", defaultRetryMax),
	}
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		logger.Errorf("invalid %s %q, using %d", key, v, fallback)

		return fallback
	}

	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		logger.Errorf("invalid %s %q, using %s", key, v, fallback)

		return fallback
	}

	return d
}
