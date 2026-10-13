package models

// Config provide values for kafka producer and consumer
type Config struct {
	// Brokers comma separated kafka brokers
	Brokers string

	// SASL provide configs for authentication
	SASL SASLConfig

	// Topics multiple topics to subscribe messages from
	// first topic will be used for publishing the message
	Topics []string
}

type SASLConfig struct {
	// User username to connect to protected kafka instance
	User string

	// Password password to connect to protected kafka instance
	Password string
}
