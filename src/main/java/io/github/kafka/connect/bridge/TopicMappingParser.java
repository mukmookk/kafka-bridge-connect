package io.github.kafka.connect.bridge;

import org.apache.kafka.common.config.ConfigException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses source-to-target topic mappings of the form:
 * sourceA:targetA,sourceB:targetB
 */
public final class TopicMappingParser {

    private TopicMappingParser() {
    }

    public static Map<String, String> parse(String rawMapping) {
        if (rawMapping == null || rawMapping.isBlank()) {
            return Collections.emptyMap();
        }

        Map<String, String> mappings = new LinkedHashMap<>();
        String[] entries = rawMapping.split(",");

        for (String entry : entries) {
            String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }

            String[] parts = trimmedEntry.split(":", 2);
            if (parts.length != 2) {
                throw new ConfigException("Invalid topic.mapping entry '" + trimmedEntry
                        + "'. Expected the format 'sourceTopic:targetTopic'.");
            }

            String sourceTopic = parts[0].trim();
            String targetTopic = parts[1].trim();

            if (sourceTopic.isEmpty() || targetTopic.isEmpty()) {
                throw new ConfigException("Invalid topic.mapping entry '" + trimmedEntry
                        + "'. Source topic and target topic must both be non-empty.");
            }

            String previous = mappings.putIfAbsent(sourceTopic, targetTopic);
            if (previous != null) {
                throw new ConfigException("Duplicate source topic '" + sourceTopic
                        + "' found in topic.mapping.");
            }
        }

        return Collections.unmodifiableMap(mappings);
    }
}
