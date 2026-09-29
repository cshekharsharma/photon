package singleflight_test

import (
	"context"
	"fmt"

	"github.com/cshekharsharma/photon/coordination/concurrency/singleflight"
)

func ExampleDoTyped() {
	group := singleflight.NewGroup()

	value, shared, err := singleflight.DoTyped(context.Background(), group, "config:tenant-1", func(context.Context) (string, error) {
		return "loaded", nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(value)
	fmt.Println(shared)
	// Output:
	// loaded
	// false
}
