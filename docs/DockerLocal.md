---
id: running-kafka-in-development
title: Running Kafka in Development
---

The following configuration will be used in the examples listed here. For more complete and up-to-date documentation, see [bitnami/bitnami-docker-kafka](https://github.com/bitnami/bitnami-docker-kafka).

To run this Kafka configuration, [`docker`](https://docs.docker.com/) with [`docker-compose`](https://docs.docker.com/compose/install/) is required.

Save the following file as `docker-compose.yml` in the root of your project.

```yml
version: '3'
services:
  zookeeper:
    image: zookeeper:latest
    ports:
      - "2181:2181"
  kafka:
    image: bitnami/kafka:2.11-1.1.1
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_LISTENERS: 'PLAINTEXT://:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://127.0.0.1:9092'
      ALLOW_PLAINTEXT_LISTENER: 'yes'
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
      KAFKA_CREATE_TOPICS: 'topic-test:1:1'
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

You will now be able to connect to your Kafka broker at `localhost:9092`. See the [Producer example](ProducerExample.md) to learn how to connect to and use your new Kafka broker.

## SSL & authentication methods

To configure Kafka to use SSL and/or authentication methods such as SASL, see [docker-compose.yml](https://github.com/tulios/kafkajs/blob/master/docker-compose.2_4.yml). This configuration is used while developing KafkaJS, and is more complicated to set up, but may give you a more production-like development environment.
