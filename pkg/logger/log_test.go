package logger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// useBufferLogger swaps the package-level logger for one writing to a buffer
// at the given level and restores the original when the test ends.
func useBufferLogger(t *testing.T, lvl string) *bytes.Buffer {
	t.Helper()

	b := new(bytes.Buffer)
	prev := defaultLogger
	defaultLogger = NewMockLogger(b)
	defaultLogger.level = getLevel(lvl)

	t.Cleanup(func() { defaultLogger = prev })

	return b
}

func TestLevelLabels(t *testing.T) {
	testCases := []struct {
		name  string
		logFn func()
		label string
	}{
		{"Info", func() { Infof("hello") }, "[INFO] hello"},
		{"Log", func() { Logf("hello") }, "[INFO] hello"},
		{"Print", func() { Printf("hello") }, "[INFO] hello"},
		{"Debug", func() { Debugf("hello") }, "[DEBUG] hello"},
		{"Warn", func() { Warnf("hello") }, "[WARN] hello"},
		{"Error", func() { Errorf("hello") }, "[ERROR] hello"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := useBufferLogger(t, "DEBUG")

			tc.logFn()

			if !strings.Contains(b.String(), tc.label) {
				t.Errorf("expected %q in output, got %q", tc.label, b.String())
			}
		})
	}
}

func TestLevelFiltering(t *testing.T) {
	testCases := []struct {
		level   string
		logFn   func()
		logged  bool
		message string
	}{
		{"WARN", func() { Debugf("hidden") }, false, "hidden"},
		{"WARN", func() { Infof("hidden") }, false, "hidden"},
		{"WARN", func() { Warnf("shown") }, true, "shown"},
		{"WARN", func() { Errorf("shown") }, true, "shown"},
		{"FATAL", func() { Warnf("hidden") }, false, "hidden"},
		{"FATAL", func() { Errorf("hidden") }, false, "hidden"},
		{"DEBUG", func() { Debugf("shown") }, true, "shown"},
	}

	for i, tc := range testCases {
		b := useBufferLogger(t, tc.level)

		tc.logFn()

		if got := strings.Contains(b.String(), tc.message); got != tc.logged {
			t.Errorf("[TESTCASE%d] level %s: logged=%v, want %v (output %q)", i+1, tc.level, got, tc.logged, b.String())
		}
	}
}

func TestFormatting(t *testing.T) {
	testCases := []struct {
		name  string
		logFn func()
		want  string
	}{
		{"printf verbs", func() { Infof("hello %v-%d", "world", 7) }, "[INFO] hello world-7"},
		{"print operands", func() { Info(1, 2) }, "[INFO] 1 2"},
		{"percent in message is not a verb", func() { Infof("%s", "100%d done") }, "[INFO] 100%d done"},
		{"nil argument", func() { Warnf("hello %v", nil) }, "[WARN] hello <nil>"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := useBufferLogger(t, "DEBUG")

			tc.logFn()

			if !strings.Contains(b.String(), tc.want) {
				t.Errorf("expected %q in output, got %q", tc.want, b.String())
			}
		})
	}
}

func TestUpdateAndLevel(t *testing.T) {
	l := NewMockLogger(new(bytes.Buffer))

	for _, want := range []string{"WARN", "ERROR", "DEBUG", "INFO"} {
		l.Update(want)

		if got := l.Level(); got != want {
			t.Errorf("Level() = %q after Update(%q)", got, want)
		}
	}
}

func TestFatalfExitsNonZero(t *testing.T) {
	if os.Getenv("LOGGING_FATALF_MESSAGE") == "1" {
		Fatalf("error: %v", "logging a fatal message")

		return
	}

	var out bytes.Buffer

	//nolint:gosec // launching the test binary as a subprocess to observe os.Exit(1)
	cmd := exec.Command(os.Args[0], "-test.run=TestFatalfExitsNonZero")
	cmd.Env = append(os.Environ(), "LOGGING_FATALF_MESSAGE=1")
	cmd.Stdout = &out

	err := cmd.Run()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit status 1, got %v", err)
	}

	want := fmt.Sprintf("error: %v", "logging a fatal message")
	if !strings.Contains(out.String(), want) {
		t.Errorf("expected %q in output, got %q", want, out.String())
	}
}
