# kafka-bridge-connect

`kafka-bridge-connect` is a Kafka Connect sink plugin that reads records from a source Kafka cluster and writes them to a target Kafka cluster.

## Features

- consumes source topics through Kafka Connect
- produces directly to a target Kafka cluster
- supports topic rename through `topic.mapping`
- copies key, value, and headers
- preserves tombstones
- optionally forwards timestamps
- supports producer retry, backoff, and batching settings
- optionally routes failed records to `error.topic`

## Build

Requirements:

- JDK 17+
- Maven 3.9+

```bash
mvn -q -DskipTests package
```

Artifacts:

- `target/plugin/kafka-bridge-connect-1.0.0/kafka-bridge-connect-1.0.0.jar`
- `target/plugin/kafka-bridge-connect-1.0.0/libs/`

## Deploy

1. Copy `target/plugin/kafka-bridge-connect-1.0.0/` into a directory under Kafka Connect `plugin.path`.
2. Restart the Kafka Connect worker.
3. Verify that the plugin is visible.

```bash
curl http://<connect-host>:8083/connector-plugins
```

Connector class:

```text
io.github.kafka.connect.bridge.KafkaBridgeSinkConnector
```

## Configuration

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `topics` | `true` | - | source topic list |
| `target.bootstrap.servers` | `true` | - | target Kafka bootstrap servers |
| `target.topic` | `true` | - | default target topic |
| `target.security.protocol` | `false` | `Kafka producer default` | target cluster security protocol |
| `target.sasl.mechanism` | `false` | `empty` | SASL mechanism |
| `target.sasl.jaas.config` | `false` | `empty` | SASL JAAS config |
| `target.client.id` | `false` | `generated` | producer client.id |
| `target.acks` | `false` | `Kafka producer default` | producer acks |
| `target.enable.idempotence` | `false` | `Kafka producer default` | enable idempotent producer |
| `target.max.in.flight.requests.per.connection` | `false` | `Kafka producer default` | max in-flight requests |
| `target.compression.type` | `false` | `Kafka producer default` | compression type |
| `target.retries` | `false` | `Kafka producer default` | producer retry count |
| `target.retry.backoff.ms` | `false` | `Kafka producer default` | retry backoff in milliseconds |
| `target.delivery.timeout.ms` | `false` | `Kafka producer default` | delivery timeout in milliseconds |
| `target.linger.ms` | `false` | `Kafka producer default` | producer linger time in milliseconds |
| `target.batch.size` | `false` | `Kafka producer default` | producer batch size in bytes |
| `topic.mapping` | `false` | `empty` | source-to-target topic mapping |
| `copy.headers` | `false` | `true` | copy headers to target records |
| `preserve.timestamp` | `false` | `false` | forward source timestamp |
| `error.topic` | `false` | `empty` | target topic for failed records |
| `fail.on.serialization.error` | `false` | `true` | fail the task on serialization errors when `error.topic` is not set |

Example:

```properties
topic.mapping=orders.created:orders-created-replica,orders.updated:orders-updated-replica
```

## Example Configuration

Example file:

- [`examples/kafka-bridge-orders.json`](./examples/kafka-bridge-orders.json)

```json
{
  "name": "kafka-bridge-orders",
  "config": {
    "connector.class": "io.github.kafka.connect.bridge.KafkaBridgeSinkConnector",
    "tasks.max": "1",
    "topics": "orders.created,orders.updated",
    "target.topic": "orders-replica",
    "target.bootstrap.servers": "target-kafka.example.com:9092",
    "target.security.protocol": "PLAINTEXT",
    "topic.mapping": "orders.created:orders-created-replica,orders.updated:orders-updated-replica",
    "copy.headers": "true",
    "preserve.timestamp": "true",
    "error.topic": "orders-bridge-errors",
    "fail.on.serialization.error": "true",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter.schemas.enable": "false"
  }
}
```
