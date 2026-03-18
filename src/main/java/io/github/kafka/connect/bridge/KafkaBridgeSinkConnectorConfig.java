package io.github.kafka.connect.bridge;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Centralized connector/task configuration definition and validation.
 */
public class KafkaBridgeSinkConnectorConfig extends AbstractConfig {

    public static final String TOPICS_CONFIG = "topics";
    public static final String TARGET_BOOTSTRAP_SERVERS_CONFIG = "target.bootstrap.servers";
    public static final String TARGET_TOPIC_CONFIG = "target.topic";
    public static final String TARGET_SECURITY_PROTOCOL_CONFIG = "target.security.protocol";
    public static final String TARGET_SASL_MECHANISM_CONFIG = "target.sasl.mechanism";
    public static final String TARGET_SASL_JAAS_CONFIG = "target.sasl.jaas.config";
    public static final String TARGET_CLIENT_ID_CONFIG = "target.client.id";
    public static final String TARGET_ACKS_CONFIG = "target.acks";
    public static final String TARGET_ENABLE_IDEMPOTENCE_CONFIG = "target.enable.idempotence";
    public static final String TARGET_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG =
            "target.max.in.flight.requests.per.connection";
    public static final String TARGET_COMPRESSION_TYPE_CONFIG = "target.compression.type";
    public static final String TARGET_RETRIES_CONFIG = "target.retries";
    public static final String TARGET_RETRY_BACKOFF_MS_CONFIG = "target.retry.backoff.ms";
    public static final String TARGET_DELIVERY_TIMEOUT_MS_CONFIG = "target.delivery.timeout.ms";
    public static final String TARGET_LINGER_MS_CONFIG = "target.linger.ms";
    public static final String TARGET_BATCH_SIZE_CONFIG = "target.batch.size";
    public static final String TOPIC_MAPPING_CONFIG = "topic.mapping";
    public static final String COPY_HEADERS_CONFIG = "copy.headers";
    public static final String PRESERVE_TIMESTAMP_CONFIG = "preserve.timestamp";
    public static final String FAIL_ON_SERIALIZATION_ERROR_CONFIG = "fail.on.serialization.error";
    public static final String ERROR_TOPIC_CONFIG = "error.topic";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    TOPICS_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Comma-separated source topics consumed by the Kafka Connect worker."
            )
            .define(
                    TARGET_BOOTSTRAP_SERVERS_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Bootstrap servers of the target Kafka cluster."
            )
            .define(
                    TARGET_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Default target topic used when topic.mapping does not contain the source topic."
            )
            .define(
                    TARGET_SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka security protocol."
            )
            .define(
                    TARGET_SASL_MECHANISM_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional SASL mechanism for the target Kafka cluster."
            )
            .define(
                    TARGET_SASL_JAAS_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional SASL JAAS configuration for the target Kafka cluster."
            )
            .define(
                    TARGET_CLIENT_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Optional Kafka producer client.id for the target cluster."
            )
            .define(
                    TARGET_ACKS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer acks setting."
            )
            .define(
                    TARGET_ENABLE_IDEMPOTENCE_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer enable.idempotence setting."
            )
            .define(
                    TARGET_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer max.in.flight.requests.per.connection setting."
            )
            .define(
                    TARGET_COMPRESSION_TYPE_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Optional compression.type for the target producer."
            )
            .define(
                    TARGET_RETRIES_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer retries setting."
            )
            .define(
                    TARGET_RETRY_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer retry.backoff.ms setting."
            )
            .define(
                    TARGET_DELIVERY_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer delivery.timeout.ms setting."
            )
            .define(
                    TARGET_LINGER_MS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer linger.ms setting."
            )
            .define(
                    TARGET_BATCH_SIZE_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional override for the target Kafka producer batch.size setting."
            )
            .define(
                    TOPIC_MAPPING_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional source-to-target topic mapping in the form sourceA:targetA,sourceB:targetB."
            )
            .define(
                    COPY_HEADERS_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "Whether to copy Kafka Connect headers to the target Kafka record."
            )
            .define(
                    PRESERVE_TIMESTAMP_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.MEDIUM,
                    "Whether to forward the source record timestamp to the target record."
            )
            .define(
                    FAIL_ON_SERIALIZATION_ERROR_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "Whether to fail the task when key/value/header serialization fails and no error topic is configured."
            )
            .define(
                    ERROR_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Optional target-cluster topic that receives JSON failure envelopes for records that could not be bridged."
            );

    public KafkaBridgeSinkConnectorConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public void validateBridgeConfig() {
        List<String> topics = getList(TOPICS_CONFIG);
        if (topics == null || topics.isEmpty() || topics.stream().allMatch(String::isBlank)) {
            throw new ConfigException("The '" + TOPICS_CONFIG + "' setting is required and must contain at least one source topic.");
        }

        String bootstrapServers = getString(TARGET_BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new ConfigException("The '" + TARGET_BOOTSTRAP_SERVERS_CONFIG + "' setting is required and must not be blank.");
        }

        String targetTopic = getString(TARGET_TOPIC_CONFIG);
        if (targetTopic == null || targetTopic.isBlank()) {
            throw new ConfigException("The '" + TARGET_TOPIC_CONFIG + "' setting is required and must not be blank.");
        }

        validateSecurityProtocolIfPresent();
        TopicMappingParser.parse(getString(TOPIC_MAPPING_CONFIG));

        validateIntegerIfPresent(TARGET_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG, 1, 5);
        validateIntegerIfPresent(TARGET_RETRIES_CONFIG, 0, Integer.MAX_VALUE);
        validateLongIfPresent(TARGET_RETRY_BACKOFF_MS_CONFIG, 0L, Long.MAX_VALUE);
        validateIntegerIfPresent(TARGET_DELIVERY_TIMEOUT_MS_CONFIG, 1, Integer.MAX_VALUE);
        validateLongIfPresent(TARGET_LINGER_MS_CONFIG, 0L, Long.MAX_VALUE);
        validateIntegerIfPresent(TARGET_BATCH_SIZE_CONFIG, 1, Integer.MAX_VALUE);

        Boolean idempotenceEnabled = optionalBoolean(TARGET_ENABLE_IDEMPOTENCE_CONFIG);
        String acks = optionalString(TARGET_ACKS_CONFIG);
        if (Boolean.TRUE.equals(idempotenceEnabled) && acks != null
                && !"all".equalsIgnoreCase(acks) && !"-1".equals(acks)) {
            throw new ConfigException("The '" + TARGET_ACKS_CONFIG + "' setting must be 'all' (or '-1') when '"
                    + TARGET_ENABLE_IDEMPOTENCE_CONFIG + "' is true.");
        }

        String errorTopic = getString(ERROR_TOPIC_CONFIG);
        if (errorTopic != null && !errorTopic.isBlank() && errorTopic.equals(targetTopic)) {
            throw new ConfigException("The '" + ERROR_TOPIC_CONFIG + "' setting must not be the same as '" + TARGET_TOPIC_CONFIG + "'.");
        }
    }

    public Map<String, String> topicMappings() {
        return TopicMappingParser.parse(getString(TOPIC_MAPPING_CONFIG));
    }

    public Map<String, Object> buildProducerProperties(String defaultClientId) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getString(TARGET_BOOTSTRAP_SERVERS_CONFIG));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        putIfNotBlank(producerProps, ProducerConfig.ACKS_CONFIG, getString(TARGET_ACKS_CONFIG));
        putIfPresentBoolean(producerProps, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, TARGET_ENABLE_IDEMPOTENCE_CONFIG);
        putIfPresentInteger(
                producerProps,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                TARGET_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG
        );
        putIfPresentInteger(producerProps, ProducerConfig.RETRIES_CONFIG, TARGET_RETRIES_CONFIG);
        putIfPresentLong(producerProps, ProducerConfig.RETRY_BACKOFF_MS_CONFIG, TARGET_RETRY_BACKOFF_MS_CONFIG);
        putIfPresentInteger(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TARGET_DELIVERY_TIMEOUT_MS_CONFIG);
        putIfPresentLong(producerProps, ProducerConfig.LINGER_MS_CONFIG, TARGET_LINGER_MS_CONFIG);
        putIfPresentInteger(producerProps, ProducerConfig.BATCH_SIZE_CONFIG, TARGET_BATCH_SIZE_CONFIG);
        putIfNotBlank(producerProps, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, getString(TARGET_SECURITY_PROTOCOL_CONFIG));

        String clientId = getString(TARGET_CLIENT_ID_CONFIG);
        producerProps.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                clientId == null || clientId.isBlank() ? defaultClientId : clientId
        );

        putIfNotBlank(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, getString(TARGET_COMPRESSION_TYPE_CONFIG));
        putIfNotBlank(producerProps, "sasl.mechanism", getString(TARGET_SASL_MECHANISM_CONFIG));
        putIfNotBlank(producerProps, "sasl.jaas.config", getPassword(TARGET_SASL_JAAS_CONFIG).value());

        return producerProps;
    }

    private void putIfNotBlank(Map<String, Object> target, String key, String value) {
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }

    private void putIfPresentBoolean(Map<String, Object> target, String producerKey, String configKey) {
        Boolean value = optionalBoolean(configKey);
        if (value != null) {
            target.put(producerKey, value);
        }
    }

    private void putIfPresentInteger(Map<String, Object> target, String producerKey, String configKey) {
        Integer value = optionalInteger(configKey);
        if (value != null) {
            target.put(producerKey, value);
        }
    }

    private void putIfPresentLong(Map<String, Object> target, String producerKey, String configKey) {
        Long value = optionalLong(configKey);
        if (value != null) {
            target.put(producerKey, value);
        }
    }

    private String optionalString(String configKey) {
        String value = getString(configKey);
        return value == null || value.isBlank() ? null : value;
    }

    private Boolean optionalBoolean(String configKey) {
        String value = optionalString(configKey);
        if (value == null) {
            return null;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        throw new ConfigException("The '" + configKey + "' setting must be 'true' or 'false'.");
    }

    private Integer optionalInteger(String configKey) {
        String value = optionalString(configKey);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ConfigException("The '" + configKey + "' setting must be a valid integer.", e);
        }
    }

    private Long optionalLong(String configKey) {
        String value = optionalString(configKey);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ConfigException("The '" + configKey + "' setting must be a valid long.", e);
        }
    }

    private void validateIntegerIfPresent(String configKey, int minValue, int maxValue) {
        Integer value = optionalInteger(configKey);
        if (value == null) {
            return;
        }
        if (value < minValue || value > maxValue) {
            throw new ConfigException("The '" + configKey + "' setting must be between " + minValue + " and " + maxValue + ".");
        }
    }

    private void validateLongIfPresent(String configKey, long minValue, long maxValue) {
        Long value = optionalLong(configKey);
        if (value == null) {
            return;
        }
        if (value < minValue || value > maxValue) {
            throw new ConfigException("The '" + configKey + "' setting must be between " + minValue + " and " + maxValue + ".");
        }
    }

    private void validateSecurityProtocolIfPresent() {
        String value = optionalString(TARGET_SECURITY_PROTOCOL_CONFIG);
        if (value == null) {
            return;
        }
        if ("PLAINTEXT".equals(value) || "SSL".equals(value)
                || "SASL_PLAINTEXT".equals(value) || "SASL_SSL".equals(value)) {
            return;
        }
        throw new ConfigException("The '" + TARGET_SECURITY_PROTOCOL_CONFIG
                + "' setting must be one of PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.");
    }
}
