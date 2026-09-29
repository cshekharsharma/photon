package notifyrclient

import (
	"context"
	"errors"
	"testing"

	"github.com/cshekharsharma/photon/notifier/contracts"

	cloudmessagequeue "github.com/cshekharsharma/photon/cloud/entity/messagequeue"
)

type mqStub struct {
	queueURL       string
	messageBody    string
	messageGroupID string
	dedupeID       string
	useDedupe      bool
	sendErr        error
	ctx            context.Context
}

func (s *mqStub) SendMessage(ctx context.Context, queueURL string, messageBody string, messageGroupId string) (*cloudmessagequeue.SendMessageResult, error) {
	s.ctx = ctx
	s.queueURL = queueURL
	s.messageBody = messageBody
	s.messageGroupID = messageGroupId
	if s.sendErr != nil {
		return nil, s.sendErr
	}
	return &cloudmessagequeue.SendMessageResult{}, nil
}

type dedupeMQStub struct {
	mqStub
}

func (s *dedupeMQStub) SendMessageWithDedupe(ctx context.Context, queueURL, messageBody, messageGroupID, dedupeID string) (*cloudmessagequeue.SendMessageResult, error) {
	s.ctx = ctx
	s.queueURL = queueURL
	s.messageBody = messageBody
	s.messageGroupID = messageGroupID
	s.dedupeID = dedupeID
	s.useDedupe = true
	if s.sendErr != nil {
		return nil, s.sendErr
	}
	return &cloudmessagequeue.SendMessageResult{MessageId: "provider-msg-1"}, nil
}

func (s *mqStub) ReceiveMessages(context.Context, string, int32) ([]*cloudmessagequeue.ReceiveMessageResult, error) {
	return nil, nil
}

func (s *mqStub) DeleteMessage(context.Context, string, string) (*cloudmessagequeue.DeleteMessageResult, error) {
	return &cloudmessagequeue.DeleteMessageResult{}, nil
}

func (s *mqStub) ChangeMessageVisibility(context.Context, string, string, int32) (*cloudmessagequeue.ChangeMessageVisibilityResult, error) {
	return &cloudmessagequeue.ChangeMessageVisibilityResult{}, nil
}

func (s *mqStub) GetQueueAttributes(context.Context, string) (*cloudmessagequeue.GetQueueAttributesResult, error) {
	return &cloudmessagequeue.GetQueueAttributesResult{}, nil
}

func TestPublisherRoutesEmailHighPriority(t *testing.T) {
	mq := &mqStub{}
	publisher, err := NewPublisher(mq, QueueConfig{
		EmailHighPriorityURL: "https://example.com/email-high",
	}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelEmail,
		Priority:       contracts.PriorityHigh,
		Purpose:        "mfa_login",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if mq.queueURL != "https://example.com/email-high" {
		t.Fatalf("unexpected queue url: %q", mq.queueURL)
	}
}

func TestPublisherUsesFIFOGroupID(t *testing.T) {
	mq := &mqStub{}
	publisher, err := NewPublisher(mq, QueueConfig{
		SMSHighPriorityURL: "https://example.com/sms-high.fifo",
	}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelSMS,
		Priority:       contracts.PriorityHigh,
		Purpose:        "verify_phone",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if mq.messageGroupID != "sms-high-verify_phone" {
		t.Fatalf("unexpected message group id: %q", mq.messageGroupID)
	}
}

func TestPublisherUsesFIFODedupeWhenSupported(t *testing.T) {
	mq := &dedupeMQStub{}
	var audit PublishAuditEvent
	publisher, err := NewPublisher(mq, QueueConfig{
		SMSHighPriorityURL: "https://example.com/sms-high.fifo",
	}, PublisherOptions{
		AuditHook: func(event PublishAuditEvent) { audit = event },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelSMS,
		Priority:       contracts.PriorityHigh,
		Purpose:        "verify phone",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !mq.useDedupe {
		t.Fatal("expected dedupe path")
	}
	if mq.dedupeID != "idem-1" {
		t.Fatalf("unexpected dedupe id: %q", mq.dedupeID)
	}
	if mq.messageGroupID != "sms-high-verify-phone" {
		t.Fatalf("unexpected message group id: %q", mq.messageGroupID)
	}
	if audit.Status != PublishAuditStatusSuccess || audit.ProviderID != "provider-msg-1" {
		t.Fatalf("unexpected audit event: %#v", audit)
	}
}

func TestPublisherAuditHookOnFailure(t *testing.T) {
	var audit PublishAuditEvent
	publisher, err := NewPublisher(&mqStub{sendErr: errors.New("send failed")}, QueueConfig{
		EmailHighPriorityURL: "https://example.com/email-high",
	}, PublisherOptions{
		AuditHook: func(event PublishAuditEvent) { audit = event },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), validNotificationMessage(contracts.ChannelEmail, contracts.PriorityHigh))
	if err == nil {
		t.Fatal("expected error")
	}
	if audit.Status != PublishAuditStatusFailure || audit.Err == nil {
		t.Fatalf("unexpected audit event: %#v", audit)
	}
}

func TestPublisherHelpers(t *testing.T) {
	if got := sanitizeMessageGroupPart(""); got != "default" {
		t.Fatalf("unexpected default group part: %q", got)
	}
	if got := sanitizeMessageGroupPart("a b/c"); got != "a-b-c" {
		t.Fatalf("unexpected sanitized group part: %q", got)
	}
	if got := sanitizeMessageGroupPart("AZ09_-"); got != "AZ09_-" {
		t.Fatalf("unexpected sanitized group part: %q", got)
	}

	var publisher *Publisher
	if publisher.now().IsZero() {
		t.Fatal("expected fallback clock")
	}
}

func TestNewPublisherRequiresMessageQueue(t *testing.T) {
	publisher, err := NewPublisher(nil, QueueConfig{}, PublisherOptions{})

	if err == nil {
		t.Fatal("expected error")
	}
	if publisher != nil {
		t.Fatalf("expected nil publisher, got %#v", publisher)
	}
}

func TestNewPublisherRejectsWhitespaceQueueURL(t *testing.T) {
	publisher, err := NewPublisher(&mqStub{}, QueueConfig{EmailHighPriorityURL: "   "}, PublisherOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if publisher != nil {
		t.Fatalf("expected nil publisher, got %#v", publisher)
	}
}

func TestPublisherPublishRequiresMessage(t *testing.T) {
	publisher, err := NewPublisher(&mqStub{}, QueueConfig{}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPublisherPublishReturnsResolveError(t *testing.T) {
	publisher, err := NewPublisher(&mqStub{}, QueueConfig{}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelEmail,
		Priority:       contracts.PriorityHigh,
		Purpose:        "mfa_login",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPublisherPublishReturnsMarshalError(t *testing.T) {
	publisher, err := NewPublisher(&mqStub{}, QueueConfig{
		EmailHighPriorityURL: "https://example.com/email-high",
	}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelEmail,
		Priority:       contracts.PriorityHigh,
		Purpose:        "mfa_login",
		Metadata: map[string]interface{}{
			"not_json": func() {},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPublisherPublishReturnsSendError(t *testing.T) {
	publisher, err := NewPublisher(&mqStub{sendErr: errors.New("send failed")}, QueueConfig{
		EmailHighPriorityURL: "https://example.com/email-high",
	}, PublisherOptions{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	err = publisher.Publish(context.Background(), &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        contracts.ChannelEmail,
		Priority:       contracts.PriorityHigh,
		Purpose:        "mfa_login",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPublisherResolveQueueURL(t *testing.T) {
	publisher := &Publisher{
		queues: QueueConfig{
			EmailHighPriorityURL: " https://example.com/email-high ",
			EmailLowPriorityURL:  " https://example.com/email-low ",
			SMSHighPriorityURL:   " https://example.com/sms-high ",
			SMSLowPriorityURL:    " https://example.com/sms-low ",
		},
	}

	tests := []struct {
		name    string
		msg     *contracts.NotificationMessage
		wantURL string
		wantErr bool
	}{
		{
			name: "missing required fields",
			msg: &contracts.NotificationMessage{
				MessageID:      " ",
				IdempotencyKey: "idem-1",
				Channel:        contracts.ChannelEmail,
				Priority:       contracts.PriorityHigh,
				Purpose:        "mfa_login",
			},
			wantErr: true,
		},
		{
			name:    "email high",
			msg:     validNotificationMessage(contracts.ChannelEmail, contracts.PriorityHigh),
			wantURL: "https://example.com/email-high",
		},
		{
			name:    "email low",
			msg:     validNotificationMessage(contracts.ChannelEmail, contracts.PriorityLow),
			wantURL: "https://example.com/email-low",
		},
		{
			name:    "sms high",
			msg:     validNotificationMessage(contracts.ChannelSMS, contracts.PriorityHigh),
			wantURL: "https://example.com/sms-high",
		},
		{
			name:    "sms low",
			msg:     validNotificationMessage(contracts.ChannelSMS, contracts.PriorityLow),
			wantURL: "https://example.com/sms-low",
		},
		{
			name:    "unsupported combination",
			msg:     validNotificationMessage(contracts.Channel("push"), contracts.PriorityHigh),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueURL, err := publisher.resolveQueueURL(tt.msg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve queue url: %v", err)
			}
			if queueURL != tt.wantURL {
				t.Fatalf("unexpected queue url: %q", queueURL)
			}
		})
	}
}

func TestPublisherResolveQueueURLReturnsMissingQueueErrors(t *testing.T) {
	tests := []struct {
		name string
		msg  *contracts.NotificationMessage
	}{
		{name: "email high", msg: validNotificationMessage(contracts.ChannelEmail, contracts.PriorityHigh)},
		{name: "email low", msg: validNotificationMessage(contracts.ChannelEmail, contracts.PriorityLow)},
		{name: "sms high", msg: validNotificationMessage(contracts.ChannelSMS, contracts.PriorityHigh)},
		{name: "sms low", msg: validNotificationMessage(contracts.ChannelSMS, contracts.PriorityLow)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &Publisher{}
			_, err := publisher.resolveQueueURL(tt.msg)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func validNotificationMessage(channel contracts.Channel, priority contracts.Priority) *contracts.NotificationMessage {
	return &contracts.NotificationMessage{
		MessageID:      "msg-1",
		IdempotencyKey: "idem-1",
		Channel:        channel,
		Priority:       priority,
		Purpose:        "notification",
	}
}
