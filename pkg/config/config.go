package config

import (
	"os"
	"sync"

	"github.com/d3ltmeshQ/evb/pkg/logger"
	"github.com/joho/godotenv"
)

// Config interface defines methods to get config values
type Config interface {
	Get(string) string
	GetOrDefault(string, string) string
}

type GoDotEnvProvider struct{}

var once sync.Once

// ReadConfig loads environment variables from .env files
// Load order: Environment Variable > .env.{env} > .env
func ReadConfig(confLocation string) (provider *GoDotEnvProvider, overrideFile, defaultFile string) {
	once.Do(func() {
		defaultFile = confLocation + "/.env"

		env := os.Getenv("ENV")
		if env == "" {
			env = "local"
		}

		overrideFile = confLocation + "/." + env + ".env"

		// Load override file first (higher priority)
		if err := godotenv.Load(overrideFile); err != nil {
			logger.Printf("Could not load override file %s: %v", overrideFile, err)
			overrideFile = ""
		}

		// Load default .env file
		if err := godotenv.Load(defaultFile); err != nil {
			logger.Printf("Could not load default file %s: %v", defaultFile, err)
			defaultFile = ""
		}
	})

	return &GoDotEnvProvider{}, overrideFile, defaultFile
}

func NewGoDotEnvProvider(configFolder string) *GoDotEnvProvider {
	provider, overrideFile, defaultFile := ReadConfig(configFolder)

	if overrideFile != "" {
		logger.Print("Loaded config from file overrideFile")
	}
	if defaultFile != "" {
		logger.Print("Loaded config from file defaultFile")
	}

	return provider
}

// Get returns value from environment variables
func (g *GoDotEnvProvider) Get(key string) string {
	return os.Getenv(key)
}

// GetOrDefault returns value from environment or a fallback default
func (g *GoDotEnvProvider) GetOrDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}
