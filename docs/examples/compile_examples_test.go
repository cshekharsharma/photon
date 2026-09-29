package examples

import (
	"context"
	"os"
	"time"

	cloudcontract "github.com/cshekharsharma/photon/cloud/contract"
	"github.com/cshekharsharma/photon/cloud/entity/messagequeue"

	"github.com/cshekharsharma/photon/cloud"
	"github.com/cshekharsharma/photon/cloud/entity"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/notifier/contracts"
	"github.com/cshekharsharma/photon/notifier/notifyrclient"
)

func Example_cloudClient() {
	ctx := context.Background()
	client, err := cloud.NewClient(cloud.Config{
		Vendor: cloud.CloudVendorAws,
		AuthArguments: &entity.CloudAuthArguments{
			AuthMode:  entity.AuthTypeAccessKey,
			AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Region:    os.Getenv("AWS_REGION"),
		},
	})
	if err != nil {
		return
	}

	objectStore, err := client.GetObjectStorage(ctx)
	if err != nil {
		return
	}

	_, _ = objectStore.GetObject(ctx, "my-bucket", "path/to/object.json")
}

func Example_notificationPublisher() {
	var mq cloudcontract.MessageQueueInterface = exampleMessageQueue{}
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
		return
	}

	_ = publisher.Publish(ctx, &contracts.NotificationMessage{
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

type exampleMessageQueue struct{}

func (exampleMessageQueue) SendMessage(context.Context, string, string, string) (*messagequeue.SendMessageResult, error) {
	return &messagequeue.SendMessageResult{}, nil
}

func (exampleMessageQueue) ReceiveMessages(context.Context, string, int32) ([]*messagequeue.ReceiveMessageResult, error) {
	return nil, nil
}

func (exampleMessageQueue) DeleteMessage(context.Context, string, string) (*messagequeue.DeleteMessageResult, error) {
	return &messagequeue.DeleteMessageResult{}, nil
}

func (exampleMessageQueue) ChangeMessageVisibility(context.Context, string, string, int32) (*messagequeue.ChangeMessageVisibilityResult, error) {
	return &messagequeue.ChangeMessageVisibilityResult{}, nil
}

func (exampleMessageQueue) GetQueueAttributes(context.Context, string) (*messagequeue.GetQueueAttributesResult, error) {
	return &messagequeue.GetQueueAttributesResult{}, nil
}
