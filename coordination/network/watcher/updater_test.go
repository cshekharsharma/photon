package watcher

import (
	"testing"
	"time"
)

func TestPushToContentUpdateChannel_ValidData(t *testing.T) {
	schema := &UpdaterSchema{
		ContentSource: 1,
		ContentFormat: 1,
		Content:       `{"key":"value"}`,
	}

	go func() {
		PushToContentUpdateChannel(schema, schema.ContentSource)
	}()

	select {
	case result := <-ContentUpdateChannel:
		if result == nil {
			t.Error("Expected non-nil schema")
		}
		if result != nil && result.Content != `{"key":"value"}` {
			t.Errorf("Unexpected content content: %s", result.Content)
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for ContentUpdateChannel")
	}
}

func TestPushToContentUpdateChannel_NilInput(t *testing.T) {
	select {
	case <-ContentUpdateChannel:
		t.Fatal("Expected channel to be empty before test")
	default:
	}

	PushToContentUpdateChannel(nil, 0)

	select {
	case <-ContentUpdateChannel:
		t.Error("Expected no message to be pushed for nil schema")
	case <-time.After(100 * time.Millisecond):
		// expected: nothing pushed
	}
}

func TestPushToContentUpdateChannelFn_IsSameAsPush(t *testing.T) {
	if PushToContentUpdateChannelFn == nil {
		t.Fatal("PushToContentUpdateChannelFn should not be nil")
	}
}

func TestSendUpdateEdges(t *testing.T) {
	if sendUpdate(nil, &UpdaterSchema{}) {
		t.Fatal("expected nil channel send to fail")
	}
	if sendUpdate(make(chan *UpdaterSchema, 1), nil) {
		t.Fatal("expected nil schema send to fail")
	}

	ch := make(chan *UpdaterSchema, 1)
	if !sendUpdate(ch, &UpdaterSchema{Content: "first"}) {
		t.Fatal("expected first send to succeed")
	}
	if sendUpdate(ch, &UpdaterSchema{Content: "second"}) {
		t.Fatal("expected full channel send to fail")
	}
}
