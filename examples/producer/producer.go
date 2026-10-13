package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/d3ltmeshQ/evb/pkg/logger"
	"github.com/d3ltmeshQ/evb/pubsub"
)

func printEvent(key string, value interface{}, headers map[string]string) {
	//this func is just for printing the event to be pushed
	eventJSON, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Printf("Error marshalling event for key %s: %v\n", key, err)
		return
	}
	fmt.Println()
	fmt.Printf("Event for key %s:\n%s\nHeaders: %+v\n\n", key, string(eventJSON), headers)
	fmt.Println()
}

func main() {
	psClient, err := pubsub.NewClient()
	if err != nil {
		logger.Printf("error occured in starting pubsub client: %v", err)
	}
	txID := "565"

	//formation of events
	roleEvent := createRoleEvent(txID, "35620957-ce94-43ca-9640-269878c25fe0", "testRole")
	permissionEvent := createPermissionEvent(txID, "040f0f09-894f-47b5-9032-4a03332cd7b3", "35620957-ce94-43ca-9640-269878c25fe0", "214800a5-5291-4172-9c90-9b9c263a7849", "14")

	//seeing the payload before push
	printEvent("role-key", roleEvent, nil)
	printEvent("permission-key", permissionEvent, nil)

	// roleevent and permevent both are map of string interface
	roleByte, _ := json.Marshal(roleEvent)
	roleKey, _ := json.Marshal("role-key")
	permKey, _ := json.Marshal("perm-key")
	permByte, _ := json.Marshal(permissionEvent)

	if err := psClient.Ping(); err != nil {
		// ctx.Logger.Errorf("ping failed! error : [%v]", err)
		logger.Printf("ping failed! error : [%v]", err)
		return
	}

	err = psClient.Publish(roleKey, roleByte)
	if err != nil {
		logger.Printf("pubsub error: %v", err)
	}

	err = psClient.Publish(permKey, permByte)
	if err != nil {
		logger.Printf("pubsub error: %v", err)
	}

	fmt.Println("Producer finished sending messages.")
}

func createRoleEvent(txID, roleID, roleName string) map[string]interface{} {
	return map[string]interface{}{
		"schema": map[string]interface{}{},
		"payload": map[string]interface{}{
			"after": map[string]interface{}{
				"id":         roleID,
				"name":       roleName,
				"created_by": "Amisha Sharma",
				"created_at": time.Now().UnixMicro(),
				"lookup": map[string]interface{}{
					"facility": []string{"ALL_FACILITIES"},
					"geo":      []string{"OH"},
				},
			},
			"source": map[string]interface{}{
				"table": "role_v2",
				"txId":  txID,
			},
		},
	}
}

func createPermissionEvent(txID, id, roleID, appID, scope string) map[string]interface{} {
	return map[string]interface{}{
		"schema": map[string]interface{}{},
		"payload": map[string]interface{}{
			"after": map[string]interface{}{
				"id":             id,
				"role_id":        roleID,
				"application_id": appID,
				"scope":          scope,
			},
			"source": map[string]interface{}{
				"table": "permission_v2",
				"txId":  txID,
			},
		},
	}
}
