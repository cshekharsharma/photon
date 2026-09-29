package config

import (
	"context"
	"time"
)

// Config interface defines a set of methods for managing application configuration.
// It provides methods to retrieve and set configuration values in a type-safe way,
// as well as methods to handle complex data types and structures such as slices and maps.
//
// Methods:
// - RawStore: Returns the raw, underlying storage mechanism used to store configuration data.
//
// - Unmarshal: Populates a struct with configuration data based on a specified path within the configuration store.
//
// - Get: Retrieves a value from the configuration as an interface{} based on the given key.
// - Set: Sets a value in the configuration store under the specified key.
//
// - GetBool: Retrieves a boolean value from the configuration.
// - GetInt64: Retrieves an int64 value from the configuration.
// - GetFloat64: Retrieves a float64 value from the configuration.
// - GetString: Retrieves a string value from the configuration.
//
// - GetTime: Retrieves a time value formatted according to the specified layout.
// - GetDuration: Retrieves a duration value from the configuration.
//
// - GetBoolSlice: Retrieves a slice of boolean values from the configuration.
// - GetInt64Slice: Retrieves a slice of int64 values from the configuration.
// - GetFloat64Slice: Retrieves a slice of float64 values from the configuration.
// - GetStringSlice: Retrieves a slice of string values from the configuration.
//
// - GetBoolMap: Retrieves a map of string keys to boolean values.
// - GetInt64Map: Retrieves a map of string keys to int64 values.
// - GetFloat64Map: Retrieves a map of string keys to float64 values.
// - GetStringMap: Retrieves a map of string keys to string values.
// - GetStringSliceMap: Retrieves a map of string keys to slices of string values.
//
// Usage:
// This interface is used to abstract the details of configuration handling from the rest of the application.
// It allows for easy access to configuration values while maintaining flexibility in the storage mechanism.
//
// Example:
//
//	func setupApp(cfg Config) {
//	    port := cfg.GetInt64("server.port")
//	    if cfg.GetBool("features.logging") {
//	        setupLogging()
//	    }
//	    fmt.Println("Starting server on port:", port)
//	}
//
// This interface can be implemented by different types of configuration providers (e.g., from files, environment variables, etc.),
// allowing for versatile and interchangeable configuration management strategies in applications.
type Config interface {
	RawStore() interface{}
	Unmarshal(path string, cfg interface{}) error

	Get(key string) interface{}
	Set(key string, value interface{}) error

	GetBool(key string) bool
	GetInt64(key string) int64
	GetFloat64(key string) float64
	GetString(key string) string

	GetTime(key string, layout string) time.Time
	GetDuration(key string) time.Duration

	GetBoolSlice(key string) []bool
	GetInt64Slice(key string) []int64
	GetFloat64Slice(key string) []float64
	GetStringSlice(key string) []string

	GetBoolMap(key string) map[string]bool
	GetInt64Map(key string) map[string]int64
	GetFloat64Map(key string) map[string]float64
	GetStringMap(key string) map[string]string
	GetStringSliceMap(key string) map[string][]string

	watchUpdaterChannel(ctx context.Context)
}
