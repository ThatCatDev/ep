package kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type KafkaConfig struct {
	ConsumerGroupName        string
	BootstrapServers         string
	SaslMechanism            *string
	SecurityProtocol         *string
	Username                 *string
	Password                 *string
	ConsumerSessionTimeoutMs *int
	ConsumerAutoOffsetReset  *string
	ClientID                 *string
	Debug                    *string
}

func GetKafkaConsumerConfig(config KafkaConfig) *kafka.ConfigMap {
	cfg := GetKafkaConfig(config)
	hydrateIfNotNil(cfg, "group.id", &config.ConsumerGroupName)
	hydrateIfNotNil(cfg, "auto.offset.reset", config.ConsumerAutoOffsetReset)
	hydrateIfNotNil(cfg, "session.timeout.ms", config.ConsumerSessionTimeoutMs)
	hydrateIfNotNil(cfg, "debug", config.Debug)

	return cfg
}

func GetKafkaConfig(kafkaConfig KafkaConfig) *kafka.ConfigMap {
	cfg := &kafka.ConfigMap{}

	hydrateIfNotNil(cfg, "bootstrap.servers", &kafkaConfig.BootstrapServers)
	hydrateIfNotNil(cfg, "sasl.mechanisms", kafkaConfig.SaslMechanism)
	hydrateIfNotNil(cfg, "security.protocol", kafkaConfig.SecurityProtocol)
	hydrateIfNotNil(cfg, "sasl.username", kafkaConfig.Username)
	hydrateIfNotNil(cfg, "sasl.password", kafkaConfig.Password)
	hydrateIfNotNil(cfg, "debug", kafkaConfig.Debug)
	hydrateIfNotNil(cfg, "client.id", kafkaConfig.ClientID)

	return cfg
}

func hydrateIfNotNil[T any](cfg *kafka.ConfigMap, key string, value *T) {
	if value == nil {
		return
	}
	// looked at the source code, as of now, there's no error being returned, it's always nil
	_ = cfg.SetKey(key, *value)
}
