package logger

import (
	"encoding/json"
	"fmt"
)

func Example() {
	Infof("Starting the application...")
	Log("This is a basic log without formatting")
	Logf("User %s has logged in", "Amisha")

	Warnf("This is a warning about memory usage")
	Errorf("An error occurred while reading the file")

	Update("DEBUG") // Change level at runtime
	Debugf("Now debug logs are also visible")

	stats := defaultLogger.System
	jsonBytes, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		Fatal(err)
	}

	Infof("System Stats")
	fmt.Println(string(jsonBytes))

	key, val := EncryptWithKeyID("amishasharma@gmail.com")
	Infof("User email (encrypted): key=%s value=%s", key, val)

	Fatalf("Fatal error: shutting down")
}
