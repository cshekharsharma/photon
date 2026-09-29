package notifyrclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cloudmessagequeue "github.com/cshekharsharma/photon/cloud/entity/messagequeue"
	"github.com/cshekharsharma/photon/notifier/contracts"

	cloudcontract "github.com/cshekharsharma/photon/cloud/contract"
)

type QueueConfig struct {
	EmailHighPriorityURL string
	EmailLowPriorityURL  string
	SMSHighPriorityURL   string
	SMSLowPriorityURL    string
}

func (q QueueConfig) Validate() error {
	queueURLs := []string{
		q.EmailHighPriorityURL,
		q.EmailLowPriorityURL,
		q.SMSHighPriorityURL,
		q.SMSLowPriorityURL,
	}
	for _, queueURL := range queueURLs {
		if queueURL != "" && strings.TrimSpace(queueURL) == "" {
			return fmt.Errorf("queue url cannot be only whitespace")
		}
	}
	return nil
}

type DeduplicatedMessageQueue interface {
	cloudcontract.MessageQueueInterface
	SendMessageWithDedupe(ctx context.Context, queueURL, messageBody, messageGroupID, dedupeID string) (*cloudmessagequeue.SendMessageResult, error)
}

type PublishAuditStatus string

const (
	PublishAuditStatusSuccess PublishAuditStatus = "success"
	PublishAuditStatusFailure PublishAuditStatus = "failure"
)

type PublishAuditEvent struct {
	MessageID      string
	IdempotencyKey string
	QueueURL       string
	Channel        contracts.Channel
	Priority       contracts.Priority
	Purpose        string
	Status         PublishAuditStatus
	ProviderID     string
	Err            error
	Duration       time.Duration
	At             time.Time
}

type PublishAuditHook func(PublishAuditEvent)

type PublisherOptions struct {
	AuditHook PublishAuditHook
	Clock     func() time.Time
}

type Publisher struct {
	mq      cloudcontract.MessageQueueInterface
	queues  QueueConfig
	options PublisherOptions
}

func NewPublisher(mq cloudcontract.MessageQueueInterface, queues QueueConfig, opts PublisherOptions) (*Publisher, error) {
	if mq == nil {
		return nil, fmt.Errorf("message queue is required")
	}
	if err := queues.Validate(); err != nil {
		return nil, err
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Publisher{
		mq:      mq,
		queues:  queues,
		options: opts,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, msg *contracts.NotificationMessage) error {
	startedAt := p.now()
	if err := msg.Validate(); err != nil {
		p.emitAudit(PublishAuditEvent{Status: PublishAuditStatusFailure, Err: err, At: startedAt})
		return err
	}
	queueURL, err := p.resolveQueueURL(msg)
	if err != nil {
		p.emitAudit(p.auditEvent(msg, queueURL, PublishAuditStatusFailure, "", err, startedAt))
		return err
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		wrapped := fmt.Errorf("marshal notification message: %w", err)
		p.emitAudit(p.auditEvent(msg, queueURL, PublishAuditStatusFailure, "", wrapped, startedAt))
		return wrapped
	}

	messageGroupID := ""
	if strings.HasSuffix(strings.TrimSpace(queueURL), ".fifo") {
		messageGroupID = string(msg.Channel) + "-" + string(msg.Priority) + "-" + sanitizeMessageGroupPart(msg.Purpose)
	}

	var result *cloudmessagequeue.SendMessageResult
	if messageGroupID != "" {
		if dedupeMQ, ok := p.mq.(DeduplicatedMessageQueue); ok {
			result, err = dedupeMQ.SendMessageWithDedupe(ctx, queueURL, string(payload), messageGroupID, msg.IdempotencyKey)
		} else {
			result, err = p.mq.SendMessage(ctx, queueURL, string(payload), messageGroupID)
		}
	} else {
		result, err = p.mq.SendMessage(ctx, queueURL, string(payload), messageGroupID)
	}
	if err != nil {
		wrapped := fmt.Errorf("publish notification message: %w", err)
		p.emitAudit(p.auditEvent(msg, queueURL, PublishAuditStatusFailure, "", wrapped, startedAt))
		return wrapped
	}

	providerID := ""
	if result != nil {
		providerID = result.MessageId
	}
	p.emitAudit(p.auditEvent(msg, queueURL, PublishAuditStatusSuccess, providerID, nil, startedAt))
	return nil
}

func (p *Publisher) resolveQueueURL(msg *contracts.NotificationMessage) (string, error) {
	if err := msg.Validate(); err != nil {
		return "", err
	}

	var queueURL string
	var missingQueueErr string
	switch msg.Channel {
	case contracts.ChannelEmail:
		switch msg.Priority {
		case contracts.PriorityHigh:
			queueURL = p.queues.EmailHighPriorityURL
			missingQueueErr = "email high-priority queue is not configured"
		case contracts.PriorityLow:
			queueURL = p.queues.EmailLowPriorityURL
			missingQueueErr = "email low-priority queue is not configured"
		}
	case contracts.ChannelSMS:
		switch msg.Priority {
		case contracts.PriorityHigh:
			queueURL = p.queues.SMSHighPriorityURL
			missingQueueErr = "sms high-priority queue is not configured"
		case contracts.PriorityLow:
			queueURL = p.queues.SMSLowPriorityURL
			missingQueueErr = "sms low-priority queue is not configured"
		}
	}

	queueURL = strings.TrimSpace(queueURL)
	if queueURL == "" {
		return "", fmt.Errorf("%s", missingQueueErr)
	}
	return queueURL, nil
}

func (p *Publisher) auditEvent(msg *contracts.NotificationMessage, queueURL string, status PublishAuditStatus, providerID string, err error, startedAt time.Time) PublishAuditEvent {
	return PublishAuditEvent{
		MessageID:      msg.MessageID,
		IdempotencyKey: msg.IdempotencyKey,
		QueueURL:       queueURL,
		Channel:        msg.Channel,
		Priority:       msg.Priority,
		Purpose:        msg.Purpose,
		Status:         status,
		ProviderID:     providerID,
		Err:            err,
		Duration:       p.now().Sub(startedAt),
		At:             startedAt,
	}
}

func (p *Publisher) emitAudit(event PublishAuditEvent) {
	if p == nil || p.options.AuditHook == nil {
		return
	}
	p.options.AuditHook(event)
}

func (p *Publisher) now() time.Time {
	if p != nil && p.options.Clock != nil {
		return p.options.Clock()
	}
	return time.Now()
}

func sanitizeMessageGroupPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "default"
	}

	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}
