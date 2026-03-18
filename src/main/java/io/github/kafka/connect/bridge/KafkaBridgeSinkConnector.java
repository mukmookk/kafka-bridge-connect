package io.github.kafka.connect.bridge;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka Connect sink connector that consumes records from the source cluster
 * and forwards them to a target Kafka cluster with minimal extra behavior.
 */
public class KafkaBridgeSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KafkaBridgeSinkConnector.class);

    private Map<String, String> connectorProps;

    @Override
    public String version() {
        return VersionUtil.version(getClass());
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            KafkaBridgeSinkConnectorConfig config = new KafkaBridgeSinkConnectorConfig(props);
            config.validateBridgeConfig();

            this.connectorProps = new LinkedHashMap<>(props);

            log.info(
                    "Starting KafkaBridgeSinkConnector. sourceTopics={}, targetBootstrapServers={}, defaultTargetTopic={}, topicMappings={}, errorTopic={}",
                    config.getList(KafkaBridgeSinkConnectorConfig.TOPICS_CONFIG),
                    config.getString(KafkaBridgeSinkConnectorConfig.TARGET_BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(KafkaBridgeSinkConnectorConfig.TARGET_TOPIC_CONFIG),
                    config.topicMappings(),
                    config.getString(KafkaBridgeSinkConnectorConfig.ERROR_TOPIC_CONFIG)
            );
        } catch (Exception e) {
            throw new ConnectException("Failed to start KafkaBridgeSinkConnector due to invalid configuration.", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaBridgeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (connectorProps == null || connectorProps.isEmpty()) {
            throw new ConnectException("Connector has not been started, so task configuration cannot be generated.");
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new LinkedHashMap<>(connectorProps));
        }

        log.info("Prepared {} task configuration(s) for KafkaBridgeSinkConnector.", taskConfigs.size());
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping KafkaBridgeSinkConnector.");
        connectorProps = null;
    }

    @Override
    public ConfigDef config() {
        return KafkaBridgeSinkConnectorConfig.CONFIG_DEF;
    }
}
