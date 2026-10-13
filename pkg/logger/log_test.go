package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	// "evb/pkg/config"
)

func TestLogLevel(t *testing.T) {
	// c := config.MockConfig{}
	testcases := []struct {
		level  string
		output string
	}{
		{"warn", "DEBUG"}, // when log level is set to WARN, DEBUG log must not be logged
		{"fatal", "WARN"}, // when log level is set to FATAL, WARN or DEBUG log must not be logged
	}

	// level := c.Get("LOG_LEVEL")

	// defer os.Setenv("LOG_LEVEL", level)

	for i, v := range testcases {
		_ = os.Setenv("LOG_LEVEL", v.level)

		b := new(bytes.Buffer)

		Warnf("%d", 1)

		Debugf("%s", v.level)

		if strings.Contains(v.output, b.String()) {
			t.Errorf("[TESTCASE%d]failed.expected %v\tgot %v\n", i+1, b.String(), v.output)
		}
	}
}

func TestLogger_Log_JSON(t *testing.T) {
	b := new(bytes.Buffer)

	r := struct {
		Name string
		Age  int
	}{
		"Alice", 23,
	}

	data, _ := json.Marshal(r)
	Infof(string(data))

	expStr := `"message":{"Name":"Alice","Age":23}`

	if !strings.Contains(b.String(), expStr) {
		t.Errorf("failed json log test .expected %v\tgot %v\n", expStr, b.String())
	}
}

type data struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func Test_DifferentLogFormats(t *testing.T) {
	b := new(bytes.Buffer)

	testCases := []struct {
		format          string
		values          []interface{}
		appData         map[string]interface{}
		expectedLog     string
		expectedAppData string
	}{
		{"hello %v", []interface{}{"krogo"}, nil, "hello krogo", ""},
		{"hello %v%v", []interface{}{"krogo", "-v1"}, nil, "hello krogo-v1", ""},
		{"hello %v", []interface{}{"krogo", map[string]interface{}{
			"key1": "value1"}}, nil, "hello krogo", `"data":{"key1":"value1"}`},
		{"hello %v", []interface{}{"krogo", map[string]interface{}{
			"key1": "value1"}}, map[string]interface{}{
			"key2": "value2"}, "hello krogo", `"data":{"key1":"value1","key2":"value2"}`},
		{"hello %v", []interface{}{"krogo", map[string]interface{}{
			"key1": "value1"}, data{"Krogo", 1}}, map[string]interface{}{
			"key2": "value2"}, "hello krogo", `"data":{"age":1,"key1":"value1","key2":"value2","name":"Krogo"}`},
	}

	for i, v := range testCases {
		b.Reset()

		Infof(v.format, v.values...)

		if !strings.Contains(b.String(), v.expectedLog) {
			t.Errorf("TESTACASE[%v] Failed, expected: %v, got: %v", i, v.expectedLog, b.String())
		}

		if !strings.Contains(b.String(), v.expectedAppData) {
			t.Errorf("TESTACASE[%v] Failed, expected: %v, got: %v", i, v.expectedAppData, b.String())
		}
	}
}

func TestQueryLogs(t *testing.T) {
	b := new(bytes.Buffer)

	testCases := []struct {
		expectedLog []string
		duration    int64
	}{
		{[]string{"SELECT * FROM PERSONS"}, 5},
		{[]string{"SELECT * FROM PERSONS", "SELECT * FROM CUSTOMERS"}, 7},
	}

	for _, tc := range testCases {
		b.Reset()

		expected := fmt.Sprintf("%v", tc.expectedLog)
		q := struct {
			Query    string `json:"query"`
			Duration int64  `json:"duration"`
		}{expected, tc.duration}

		Debugf("query: %v", q)

		if !strings.Contains(b.String(), expected) {
			t.Errorf("FAILED, expected: %v, got: %v", expected, b.String())
		}

		expectedDurationLog := `"duration":` + strconv.Itoa(int(tc.duration))
		if !strings.Contains(b.String(), expectedDurationLog) {
			t.Errorf("FAILED, expected: %v, got: %v", expectedDurationLog, b.String())
		}
	}
}

func TestLevel(t *testing.T) {
	b := new(bytes.Buffer)

	Infof("hello")

	if !strings.Contains(b.String(), `"level":"INFO"`) {
		t.Errorf("FAILED, ecpected level: INFO, got level: %v", b.String())
	}

	b.Reset()

	Errorf("hello")

	if !strings.Contains(b.String(), `"level":"ERROR"`) {
		t.Errorf("FAILED, ecpected level: ERROR, got level: %v", b.String())
	}

	b.Reset()

	Debugf("hello")

	if !strings.Contains(b.String(), `"level":"DEBUG"`) {
		t.Errorf("FAILED, ecpected level: DEBUG, got level: %v", b.String())
	}

	b.Reset()

	Warnf("hello")

	if !strings.Contains(b.String(), `"level":"WARN"`) {
		t.Errorf("FAILED, ecpected level: WARN, got level: %v", b.String())
	}

	b.Reset()

	Warnf("hello", nil) // test for nil struct/map case

	if !strings.Contains(b.String(), `"level":"WARN"`) {
		t.Errorf("FAILED, ecpected level: WARN, got level: %v", b.String())
	}
}

// TestPerformanceLogMessage is testing logged message in case of performance log.
func TestPerformanceLogMessage(t *testing.T) {
	l := struct {
		Method   string `json:"method"`
		URI      string `json:"uri"`
		Duration int64  `json:"duration"`
	}{"GET", "/dummy", 1}

	b := new(bytes.Buffer)

	Infof("struct: %v", l)

	if !strings.Contains(b.String(), "\"message\":\"GET /dummy\"") {
		t.Errorf("Failed to log the message in performance log")
	}
}

func Test_LogFatalf(t *testing.T) {
	oldArgs := os.Args

	defer func() { os.Args = oldArgs }()

	logMsg := "logging a fatal message"
	format := "error: %v"

	if os.Getenv("LOGGING_FATALF_MESSAGE") == "1" {
		// for passing the argument in cmd
		os.Args = []string{"log", "testing"}

		// logger := newLogger(&config.GoDotEnvProvider{})
		// logger.Fatalf(format, logMsg)

		return
	}

	// create a subprocess to execute the test
	var out bytes.Buffer
	//nolint:gosec // needed to launch subprocess with argument to test os.exit(1) scenario
	cmd := exec.Command(os.Args[0], "-test.run=Test_LogFatalf")
	cmd.Env = append(os.Environ(), "LOGGING_FATALF_MESSAGE=1")
	cmd.Stdout = &out

	err := cmd.Run()
	if err == nil {
		t.Errorf("Failed expected error exit status 1 got %v", err)
	}

	if !strings.Contains(out.String(), fmt.Sprintf(format, logMsg)) {
		t.Errorf("FAILED, expected %v got %v", logMsg, out.String())
	}
}
