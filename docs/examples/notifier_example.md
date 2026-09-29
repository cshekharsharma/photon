# Notifier

Use `notifyrclient` when application code should publish notification intents to queues instead of sending email or SMS inline.

```go
package examples

import (
	"context"
	"time"

	cloudcontract "github.com/cshekharsharma/photon/cloud/contract"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/notifier/contracts"
	"github.com/cshekharsharma/photon/notifier/notifyrclient"
)

func PublishNotification(mq cloudcontract.MessageQueueInterface) error {
	ctx := context.Background()
	log := logger.Init(&logger.LoggerConfig{
		Name:     "notifier",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
		Level:    logger.LogLevelInfo,
	})

	publisher, err := notifyrclient.NewPublisher(
		mq,
		notifyrclient.QueueConfig{
			EmailHighPriorityURL: "https://sqs.ap-south-1.amazonaws.com/123/email-high.fifo",
			EmailLowPriorityURL:  "https://sqs.ap-south-1.amazonaws.com/123/email-low",
			SMSHighPriorityURL:   "https://sqs.ap-south-1.amazonaws.com/123/sms-high.fifo",
			SMSLowPriorityURL:    "https://sqs.ap-south-1.amazonaws.com/123/sms-low",
		},
		notifyrclient.PublisherOptions{
			AuditHook: func(event notifyrclient.PublishAuditEvent) {
				log.InfoWithFields(map[string]interface{}{
					"message_id":      event.MessageID,
					"idempotency_key": event.IdempotencyKey,
					"queue_url":       event.QueueURL,
					"status":          event.Status,
					"provider_id":     event.ProviderID,
					"duration_ms":     event.Duration.Milliseconds(),
				}, "notification publish completed")
			},
		},
	)
	if err != nil {
		return err
	}

	return publisher.Publish(ctx, &contracts.NotificationMessage{
		MessageID:      "msg-123",
		IdempotencyKey: "order-456:payment-success:v1",
		Channel:        contracts.ChannelEmail,
		Priority:       contracts.PriorityHigh,
		Purpose:        "payment_success",
		Recipient: contracts.Recipient{
			Email: "user@example.com",
		},
		Template: contracts.TemplateReference{
			Mode:    contracts.TemplateModeResolve,
			Channel: string(contracts.ChannelEmail),
			Purpose: "payment_success",
		},
		Variables: map[string]string{
			"order_id": "456",
		},
		SourceService: "checkout",
		Metadata: map[string]interface{}{
			"queued_at": time.Now().UTC().Format(time.RFC3339),
		},
	})
}
```

For FIFO queues, Photon uses `IdempotencyKey` as the dedupe ID when the queue client implements `SendMessageWithDedupe`.
