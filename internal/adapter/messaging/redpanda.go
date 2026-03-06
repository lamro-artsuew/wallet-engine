package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/rs/zerolog/log"
)

const (
	TopicDeposits    = "wallet.deposits"
	TopicWithdrawals = "wallet.withdrawals"
	TopicSweeps      = "wallet.sweeps"
	TopicAlerts      = "wallet.alerts"
)

// CloudEvent wraps domain events in CloudEvents 1.0 format
type CloudEvent struct {
	SpecVersion string      `json:"specversion"`
	Type        string      `json:"type"`
	Source      string      `json:"source"`
	ID          string      `json:"id"`
	Time        string      `json:"time"`
	DataSchema  string      `json:"datacontenttype"`
	Data        interface{} `json:"data"`
}

// RedpandaProducer publishes events to Redpanda (Kafka-compatible)
type RedpandaProducer struct {
	producer sarama.SyncProducer
}

// NewRedpandaProducer creates a new Redpanda producer
func NewRedpandaProducer(brokers []string) (*RedpandaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create redpanda producer: %w", err)
	}

	log.Info().Strs("brokers", brokers).Msg("connected to Redpanda")
	return &RedpandaProducer{producer: producer}, nil
}

// PublishDeposit publishes a deposit event
func (p *RedpandaProducer) PublishDeposit(ctx context.Context, d *domain.Deposit) {
	event := CloudEvent{
		SpecVersion: "1.0",
		Type:        "wallet.deposit." + string(d.State),
		Source:      "wallet-engine/" + d.Chain,
		ID:          d.ID.String(),
		Time:        time.Now().UTC().Format(time.RFC3339),
		DataSchema:  "application/json",
		Data: map[string]interface{}{
			"deposit_id":    d.ID,
			"chain":         d.Chain,
			"tx_hash":       d.TxHash,
			"from_address":  d.FromAddress,
			"to_address":    d.ToAddress,
			"token_symbol":  d.TokenSymbol,
			"amount":        d.Amount.String(),
			"state":         d.State,
			"confirmations": d.Confirmations,
			"workspace_id":  d.WorkspaceID,
			"user_id":       d.UserID,
			"block_number":  d.BlockNumber,
		},
	}

	p.publish(TopicDeposits, d.Chain, event)
}

// PublishWithdrawal publishes a withdrawal event
func (p *RedpandaProducer) PublishWithdrawal(ctx context.Context, w *domain.Withdrawal) {
	event := CloudEvent{
		SpecVersion: "1.0",
		Type:        "wallet.withdrawal." + string(w.State),
		Source:      "wallet-engine/" + w.Chain,
		ID:          w.ID.String(),
		Time:        time.Now().UTC().Format(time.RFC3339),
		DataSchema:  "application/json",
		Data: map[string]interface{}{
			"withdrawal_id": w.ID,
			"chain":         w.Chain,
			"to_address":    w.ToAddress,
			"token_symbol":  w.TokenSymbol,
			"amount":        w.Amount.String(),
			"state":         w.State,
			"workspace_id":  w.WorkspaceID,
			"user_id":       w.UserID,
		},
	}

	p.publish(TopicWithdrawals, w.Chain, event)
}

func (p *RedpandaProducer) publish(topic, key string, event CloudEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("failed to marshal event")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("failed to publish event")
		return
	}

	log.Debug().
		Str("topic", topic).
		Int32("partition", partition).
		Int64("offset", offset).
		Msg("event published")
}

// Close shuts down the producer
func (p *RedpandaProducer) Close() error {
	return p.producer.Close()
}
