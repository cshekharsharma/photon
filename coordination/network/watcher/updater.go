package watcher

var (
	ContentUpdateChannel = make(chan *UpdaterSchema, 1) // Channel for content updates callback

	// PushToContentUpdateChannelFn is a function type that takes an UpdateSchema and a content source
	PushToContentUpdateChannelFn func(update *UpdaterSchema, contentSource uint8) = PushToContentUpdateChannel
)

type UpdaterSchema struct {
	ContentSource uint8
	ContentFormat uint8
	Content       string
}

// PushToContentUpdateChannel sends an UpdaterSchema to the ContentUpdateChannel.
func PushToContentUpdateChannel(schema *UpdaterSchema, contentSource uint8) {
	if schema == nil {
		return
	}
	sendUpdate(ContentUpdateChannel, schema)
}

func sendUpdate(ch chan *UpdaterSchema, schema *UpdaterSchema) bool {
	if ch == nil || schema == nil {
		return false
	}
	select {
	case ch <- schema:
		return true
	default:
		return false
	}
}
