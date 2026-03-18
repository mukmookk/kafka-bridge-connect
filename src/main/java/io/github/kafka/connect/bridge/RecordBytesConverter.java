package io.github.kafka.connect.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Kafka Connect payloads into byte[] for the target Kafka producer.
 */
public final class RecordBytesConverter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private RecordBytesConverter() {
    }

    public static byte[] toBytes(Object value, Schema schema) throws Exception {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[] bytes) {
            return bytes;
        }

        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }

        if (value instanceof String stringValue) {
            return stringValue.getBytes(StandardCharsets.UTF_8);
        }

        Object normalized = normalize(value, schema);
        return OBJECT_MAPPER.writeValueAsBytes(normalized);
    }

    private static Object normalize(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        if (value instanceof Struct struct) {
            return structToMap(struct);
        }

        if (value instanceof Map<?, ?> mapValue) {
            Map<String, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                String key = entry.getKey() == null ? "null" : String.valueOf(entry.getKey());
                converted.put(key, normalize(entry.getValue(), null));
            }
            return converted;
        }

        if (value instanceof List<?> listValue) {
            List<Object> converted = new ArrayList<>(listValue.size());
            for (Object item : listValue) {
                converted.add(normalize(item, null));
            }
            return converted;
        }

        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }

        if (schema != null && schema.type() == Schema.Type.STRUCT && value instanceof Struct struct) {
            return structToMap(struct);
        }

        return value;
    }

    private static Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> converted = new LinkedHashMap<>();
        for (Field field : struct.schema().fields()) {
            converted.put(field.name(), normalize(struct.get(field), field.schema()));
        }
        return converted;
    }
}
