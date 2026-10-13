package logger

import (
	"reflect"
	"testing"
)

func TestGetLevel(t *testing.T) {
	testcases := []struct {
		input  string
		output level
	}{
		{"INFO", InfoLevel},
		{"WARN", WarnLevel},
		{"ERROR", ErrorLevel},
		{"FATAL", FatalLevel},
		{"DEBUG", DebugLevel},
		{"test", InfoLevel}, // default
	}

	for i, v := range testcases {
		resp := getLevel(v.input)

		if resp != v.output {
			t.Errorf("[TEST CASE %d] Failed. Expected %v\tGot %v\n", i+1, v.output, resp)
		}
	}
}

func TestLevel_String(t *testing.T) {
	testcases := []struct {
		input  level
		output string
	}{
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{FatalLevel, "FATAL"},
		{DebugLevel, "DEBUG"},
	}

	for i, v := range testcases {
		resp := v.input.String()

		if resp != v.output {
			t.Errorf("[TEST CASE %d] Failed. Expected %v\tGot %v\n", i+1, v.output, resp)
		}
	}
}

func TestLevel_MarshalJSON(t *testing.T) {
	testcases := []struct {
		input  level
		output []byte
	}{
		{InfoLevel, []byte(`"INFO"`)},
		{WarnLevel, []byte(`"WARN"`)},
		{ErrorLevel, []byte(`"ERROR"`)},
		{FatalLevel, []byte(`"FATAL"`)},
		{DebugLevel, []byte(`"DEBUG"`)},
	}

	for i, v := range testcases {
		resp, _ := v.input.MarshalJSON()

		if !reflect.DeepEqual(resp, v.output) {
			t.Errorf("[TEST CASE %d] Failed. Expected %s\tGot %s\n", i+1, v.output, resp)
		}
	}
}
