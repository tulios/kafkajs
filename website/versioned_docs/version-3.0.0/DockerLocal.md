---
id: version-3.0.0-running-kafka-in-development
title: Running Kafka in Development
original_id: running-kafka-in-development
---

Starting with Kafka 4.0, Apache Kafka runs in KRaft mode by default and no longer requires ZooKeeper. The Docker Compose configurations below reflect both the modern KRaft approach and the legacy ZooKeeper-based setup for older Kafka versions.

## KRaft Mode (Kafka 4.0+)

KRaft mode eliminates the need for a separate ZooKeeper service, simplifying your local development setup. Create a `docker-compose.yml` with the following content:

```yml
version: '2'
services:
  kafka:
    image: apache/kafka:4.2.0
    hostname: kafka
    container_name: kafka
    ports:
      - '9092:9092'
    environment:
      KAFKA_NODE_ID: '1'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_LOG_DIRS: '/tmp/kraft-logs'
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
      CLUSTER_ID: 'kafkajs-dev-cluster-id-0001'
```

Start Kafka by running:

```bash
docker compose up
```

With the KRaft single-node setup there is no need to export a `HOST_IP` variable. The broker advertises `localhost:9092` directly, so your KafkaJS client can connect out of the box:

```javascript
const kafka = new Kafka({ brokers: ['localhost:9092'] })
```

## ZooKeeper Mode (Kafka < 4.0)

If you are running an older version of Kafka that still relies on ZooKeeper, use the following `docker-compose.yml`:

```yml
version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper:latest
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka:2.13-2.8.1
    ports:
      - "9092:9092"
    links:
      - zookeeper
    environment:
      KAFKA_ADVERTISED_HOST_NAME: ${HOST_IP}
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
```

Before starting the containers, export your host IP so that the broker advertises the correct address:

```bash
export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)
docker-compose up
```

## Multi-Broker KRaft Cluster

For a more production-like setup with multiple brokers, SSL, and SASL authentication, refer to [docker-compose.4_2.yml](https://github.com/tulios/kafkajs/blob/master/docker-compose.4_2.yml) in the KafkaJS repository.

## SSL & authentication methods

To configure Kafka with SSL or SASL authentication, see the multi-broker [docker-compose.4_2.yml](https://github.com/tulios/kafkajs/blob/master/docker-compose.4_2.yml) for a working example and the [Configuration](Configuration.md#sasl) documentation for KafkaJS client settings.
