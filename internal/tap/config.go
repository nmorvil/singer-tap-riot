package tap

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	APIKeys   []string `json:"api_keys"`
	Server    string   `json:"server"`
	Players   []string `json:"players,omitempty"`
	StartDate string   `json:"start_date,omitempty"`
	QueueId   int      `json:"queue_id,omitempty"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	return &config, nil
}
