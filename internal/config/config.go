// Package config manages queuectl's persisted configuration
// (default max retries and the exponential backoff base).
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Config holds the tunable, persisted settings for the queue.
type Config struct {
	MaxRetries  int     `json:"max_retries"`
	BackoffBase float64 `json:"backoff_base"`
}

// Default returns queuectl's built-in defaults, used the first time it
// runs (before `config set` has ever been called).
func Default() Config {
	return Config{MaxRetries: 3, BackoffBase: 2}
}

// Load reads config from path, or returns Default() if no config file
// exists yet.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Default(), nil
	}
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Save writes cfg to path as JSON.
func (c Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// Set updates a single config key from a string value, as passed on the
// command line (e.g. `queuectl config set max-retries 3`).
func (c *Config) Set(key, value string) error {
	switch key {
	case "max-retries":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("max-retries must be an integer: %w", err)
		}
		c.MaxRetries = n
	case "backoff-base":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("backoff-base must be a number: %w", err)
		}
		c.BackoffBase = f
	default:
		return fmt.Errorf("unknown config key: %s (expected max-retries or backoff-base)", key)
	}
	return nil
}
