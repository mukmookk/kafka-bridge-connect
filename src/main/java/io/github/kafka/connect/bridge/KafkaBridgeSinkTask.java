package io.github.kafka.connect.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Sink task that forwards source-cluster records to a target Kafka cluster.
 *
 * Compared to the original synchronous version, this task now:
 * - serializes each record once
 * - submits all records in the current put() batch asynchronously
 * - waits for broker acknowledgements only after the batch has been fully dispatched
 *
 * This keeps Connect semantics simple while improving throughput through producer-side pipelining.
 */
public class KafkaBridgeSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaBridgeSinkTask.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KafkaProducer<byte[], byte[]> producer;
    private Map<String, String> topicMappings;
    private String defaultTargetTopic;
    private String errorTopic;
    private boolean copyHeaders;
    private boolean preserveTimestamp;
    private boolean failOnSerializationError;

    @Override
    public String version() {
        return VersionUtil.version(getClass());
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            KafkaBridgeSinkConnectorConfig config = new KafkaBridgeSinkConnectorConfig(props);
            config.validateBridgeConfig();

            this.topicMappings = config.topicMappings();
            this.defaultTargetTopic = config.getString(KafkaBridgeSinkConnectorConfig.TARGET_TOPIC_CONFIG);
            this.errorTopic = blankToNull(config.getString(KafkaBridgeSinkConnectorConfig.ERROR_TOPIC_CONFIG));
            this.copyHeaders = config.getBoolean(KafkaBridgeSinkConnectorConfig.COPY_HEADERS_CONFIG);
            this.preserveTimestamp = config.getBoolean(KafkaBridgeSinkConnectorConfig.PRESERVE_TIMESTAMP_CONFIG);
            this.failOnSerializationError = config.getBoolean(KafkaBridgeSinkConnectorConfig.FAIL_ON_SERIALIZATION_ERROR_CONFIG);

            String connectorName = props.getOrDefault("name", "kafka-bridge");
            String defaultClientId = connectorName + "-producer-" + Integer.toHexString(System.identityHashCode(this));
            Map<String, Object> producerProps = config.buildProducerProperties(defaultClientId);
            this.producer = createProducer(producerProps);

            log.info(
                    "Started KafkaBridgeSinkTask. defaultTargetTopic={}, topicMappings={}, errorTopic={}, copyHeaders={}, preserveTimestamp={}, failOnSerializationError={}, producerConfig={}",
                    defaultTargetTopic,
                    topicMappings,
                    errorTopic,
                    copyHeaders,
                    preserveTimestamp,
                    failOnSerializationError,
                    sanitizedProducerProps(producerProps)
            );
        } catch (Exception e) {
            throw new ConnectException("Failed to start KafkaBridgeSinkTask due to invalid configuration.", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        BatchOutcome outcome = new BatchOutcome(records.size());
        List<PendingSend> pendingSends = new ArrayList<>(records.size());

        for (SinkRecord record : records) {
            PreparedRecord preparedRecord = prepareRecord(record, outcome);
            if (preparedRecord == null) {
                continue;
            }

            try {
                Future<RecordMetadata> future = producer.send(preparedRecord.producerRecord());
                pendingSends.add(new PendingSend(record, preparedRecord, future));
                outcome.submittedCount++;
            } catch (Exception e) {
                handleFailedRecord(record, preparedRecord, "SEND_SUBMIT", e, outcome);
            }
        }

        awaitBatchAcknowledgements(pendingSends, outcome);
        logBatchOutcome(outcome);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (producer == null) {
            return;
        }

        try {
            producer.flush();
            int offsetCount = currentOffsets == null ? 0 : currentOffsets.size();
            log.debug("Flushed target Kafka producer for {} source partition offset(s).", offsetCount);
        } catch (Exception e) {
            throw new ConnectException("Failed to flush target Kafka producer.", e);
        }
    }

    @Override
    public void stop() {
        if (producer == null) {
            return;
        }

        try {
            log.info("Stopping KafkaBridgeSinkTask. Flushing and closing target Kafka producer.");
            producer.flush();
            producer.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            throw new ConnectException("Failed to stop KafkaBridgeSinkTask cleanly.", e);
        } finally {
            producer = null;
        }
    }

    private PreparedRecord prepareRecord(SinkRecord record, BatchOutcome outcome) {
        String targetTopic = resolveTargetTopic(record.topic());

        try {
            byte[] keyBytes = RecordBytesConverter.toBytes(record.key(), record.keySchema());
            byte[] valueBytes = RecordBytesConverter.toBytes(record.value(), record.valueSchema());
            RecordHeaders targetHeaders = copyHeaders ? copyHeaders(record.headers()) : new RecordHeaders();
            Long timestamp = preserveTimestamp ? record.timestamp() : null;

            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                    targetTopic,
                    null,
                    timestamp,
                    keyBytes,
                    valueBytes,
                    targetHeaders
            );

            return new PreparedRecord(targetTopic, keyBytes, valueBytes, targetHeaders, producerRecord);
        } catch (Exception e) {
            handleFailedRecord(record, null, "SERIALIZATION", e, outcome);
            return null;
        }
    }

    private void awaitBatchAcknowledgements(List<PendingSend> pendingSends, BatchOutcome outcome) {
        for (PendingSend pendingSend : pendingSends) {
            try {
                RecordMetadata metadata = pendingSend.future().get();
                outcome.acknowledgedCount++;
                log.debug(
                        "Forwarded record from sourceTopic={}, sourcePartition={}, sourceOffset={} to targetTopic={}, targetPartition={}, targetOffset={}",
                        pendingSend.sourceRecord().topic(),
                        pendingSend.sourceRecord().kafkaPartition(),
                        pendingSend.sourceRecord().kafkaOffset(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Interrupted while waiting for target Kafka acknowledgements.", e);
            } catch (ExecutionException e) {
                handleFailedRecord(
                        pendingSend.sourceRecord(),
                        pendingSend.preparedRecord(),
                        "SEND_ACK",
                        unwrapExecutionException(e),
                        outcome
                );
            }
        }
    }

    private void handleFailedRecord(
            SinkRecord sourceRecord,
            PreparedRecord preparedRecord,
            String failureStage,
            Exception failure,
            BatchOutcome outcome
    ) {
        if (errorTopic != null) {
            publishToErrorTopic(sourceRecord, preparedRecord, failureStage, failure);
            outcome.errorRoutedCount++;
            log.warn(
                    "Routed failed record to error topic. stage={}, sourceTopic={}, sourcePartition={}, sourceOffset={}, targetTopic={}, errorTopic={}, error={}",
                    failureStage,
                    sourceRecord.topic(),
                    sourceRecord.kafkaPartition(),
                    sourceRecord.kafkaOffset(),
                    preparedRecord == null ? resolveTargetTopic(sourceRecord.topic()) : preparedRecord.targetTopic(),
                    errorTopic,
                    failure.getMessage(),
                    failure
            );
            return;
        }

        if ("SERIALIZATION".equals(failureStage) && !failOnSerializationError) {
            outcome.skippedCount++;
            log.warn(
                    "Skipping record due to serialization error. sourceTopic={}, sourcePartition={}, sourceOffset={}, error={}",
                    sourceRecord.topic(),
                    sourceRecord.kafkaPartition(),
                    sourceRecord.kafkaOffset(),
                    failure.getMessage(),
                    failure
            );
            return;
        }

        throw wrapAsConnectException(
                "Failed to bridge record at stage=" + failureStage
                        + ", sourceTopic=" + sourceRecord.topic()
                        + ", sourcePartition=" + sourceRecord.kafkaPartition()
                        + ", sourceOffset=" + sourceRecord.kafkaOffset()
                        + ", targetTopic=" + (preparedRecord == null ? resolveTargetTopic(sourceRecord.topic()) : preparedRecord.targetTopic()) + ".",
                failure
        );
    }

    private void publishToErrorTopic(
            SinkRecord sourceRecord,
            PreparedRecord preparedRecord,
            String failureStage,
            Exception failure
    ) {
        try {
            byte[] errorPayload = OBJECT_MAPPER.writeValueAsBytes(buildErrorEnvelope(sourceRecord, preparedRecord, failureStage, failure));
            ProducerRecord<byte[], byte[]> errorRecord = new ProducerRecord<>(
                    errorTopic,
                    null,
                    null,
                    null,
                    errorPayload,
                    buildErrorHeaders(sourceRecord, failureStage)
            );
            producer.send(errorRecord).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while publishing a failure envelope to error topic '" + errorTopic + "'.", e);
        } catch (Exception e) {
            throw new ConnectException(
                    "Failed to publish a failure envelope to error topic '" + errorTopic
                            + "' for sourceTopic=" + sourceRecord.topic()
                            + ", sourcePartition=" + sourceRecord.kafkaPartition()
                            + ", sourceOffset=" + sourceRecord.kafkaOffset() + ".",
                    e
            );
        }
    }

    private Map<String, Object> buildErrorEnvelope(
            SinkRecord sourceRecord,
            PreparedRecord preparedRecord,
            String failureStage,
            Exception failure
    ) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("bridgeVersion", version());
        envelope.put("failedAt", Instant.now().toString());
        envelope.put("failureStage", failureStage);
        envelope.put("errorClass", failure.getClass().getName());
        envelope.put("errorMessage", failure.getMessage());

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("topic", sourceRecord.topic());
        source.put("partition", sourceRecord.kafkaPartition());
        source.put("offset", sourceRecord.kafkaOffset());
        source.put("timestamp", sourceRecord.timestamp());
        source.put("keyClass", sourceRecord.key() == null ? null : sourceRecord.key().getClass().getName());
        source.put("valueClass", sourceRecord.value() == null ? null : sourceRecord.value().getClass().getName());
        envelope.put("source", source);

        Map<String, Object> target = new LinkedHashMap<>();
        target.put("topic", preparedRecord == null ? resolveTargetTopic(sourceRecord.topic()) : preparedRecord.targetTopic());
        target.put("requestedPartition", null);
        envelope.put("target", target);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("keyBase64", preparedRecord == null ? null : encodeBase64(preparedRecord.keyBytes()));
        payload.put("valueBase64", preparedRecord == null ? null : encodeBase64(preparedRecord.valueBytes()));
        payload.put("headers", preparedRecord == null ? List.of() : headerPayload(preparedRecord.headers()));
        envelope.put("payload", payload);

        return envelope;
    }

    private RecordHeaders buildErrorHeaders(SinkRecord sourceRecord, String failureStage) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("bridge.failure.stage", failureStage.getBytes(StandardCharsets.UTF_8));
        headers.add("bridge.source.topic", sourceRecord.topic().getBytes(StandardCharsets.UTF_8));
        headers.add("bridge.source.partition", String.valueOf(sourceRecord.kafkaPartition()).getBytes(StandardCharsets.UTF_8));
        headers.add("bridge.source.offset", String.valueOf(sourceRecord.kafkaOffset()).getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private List<Map<String, Object>> headerPayload(RecordHeaders headers) {
        List<Map<String, Object>> headerPayload = new ArrayList<>();
        headers.forEach(header -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("key", header.key());
            value.put("valueBase64", encodeBase64(header.value()));
            headerPayload.add(value);
        });
        return headerPayload;
    }

    private String encodeBase64(byte[] value) {
        return value == null ? null : Base64.getEncoder().encodeToString(value);
    }

    private KafkaProducer<byte[], byte[]> createProducer(Map<String, Object> producerProps) {
        return new KafkaProducer<>(producerProps);
    }

    private String resolveTargetTopic(String sourceTopic) {
        return topicMappings.getOrDefault(sourceTopic, defaultTargetTopic);
    }

    private RecordHeaders copyHeaders(Headers sourceHeaders) throws Exception {
        RecordHeaders targetHeaders = new RecordHeaders();
        if (sourceHeaders == null) {
            return targetHeaders;
        }

        for (Header header : sourceHeaders) {
            byte[] valueBytes = RecordBytesConverter.toBytes(header.value(), header.schema());
            targetHeaders.add(header.key(), valueBytes);
        }
        return targetHeaders;
    }

    private Map<String, Object> sanitizedProducerProps(Map<String, Object> rawProps) {
        Map<String, Object> sanitized = new LinkedHashMap<>(rawProps);
        if (sanitized.containsKey("sasl.jaas.config")) {
            sanitized.put("sasl.jaas.config", "[hidden]");
        }
        return sanitized;
    }

    private Exception unwrapExecutionException(ExecutionException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof Exception wrapped) {
            return wrapped;
        }
        return new Exception(cause == null ? exception.getMessage() : cause.getMessage(), cause);
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void logBatchOutcome(BatchOutcome outcome) {
        if (outcome.errorRoutedCount > 0 || outcome.skippedCount > 0) {
            log.warn(
                    "Completed Kafka bridge batch with partial issues. polled={}, submitted={}, acknowledged={}, errorRouted={}, skipped={}",
                    outcome.polledCount,
                    outcome.submittedCount,
                    outcome.acknowledgedCount,
                    outcome.errorRoutedCount,
                    outcome.skippedCount
            );
            return;
        }

        log.debug(
                "Completed Kafka bridge batch. polled={}, submitted={}, acknowledged={}",
                outcome.polledCount,
                outcome.submittedCount,
                outcome.acknowledgedCount
        );
    }

    private ConnectException wrapAsConnectException(String message, Exception exception) {
        if (exception instanceof ConnectException connectException) {
            return new ConnectException(message, connectException);
        }
        return new ConnectException(message, exception);
    }

    private record PreparedRecord(
            String targetTopic,
            byte[] keyBytes,
            byte[] valueBytes,
            RecordHeaders headers,
            ProducerRecord<byte[], byte[]> producerRecord
    ) {
    }

    private record PendingSend(
            SinkRecord sourceRecord,
            PreparedRecord preparedRecord,
            Future<RecordMetadata> future
    ) {
    }

    private static final class BatchOutcome {
        private final int polledCount;
        private int submittedCount;
        private int acknowledgedCount;
        private int errorRoutedCount;
        private int skippedCount;

        private BatchOutcome(int polledCount) {
            this.polledCount = polledCount;
        }
    }
}
