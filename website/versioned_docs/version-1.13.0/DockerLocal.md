---
id: version-1.13.0-running-kafka-in-development
title: Running Kafka in Development
original_id: running-kafka-in-development
---

The following configuration will be used in the examples listed here. For more complete and up-to-date documentation, see [wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker).

To run this Kafka configuration, [`docker`](https://docs.docker.com/) with [`docker-compose`](https://docs.docker.com/compose/install/) is required.

Save the following file as `docker-compose.yml` in the root of your project.

```yml
version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper:latest
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka:2.11-1.1.1
    ports:
      - "9092:9092"
    links:
      - zookeeper
    environment:
      KAFKA_ADVERTISED_HOST_NAME: ${HOST_IP}
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
      KAFKA_CREATE_TOPICS: "topic-test:1:1"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

Now run:

```sh
export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)
docker-compose up
```

> Note: To understand why `HOST_IP` is required, see the [kafka-docker connectivity guide](https://github.com/wurstmeister/kafka-docker/wiki/Connectivity)

You will now be able to connect to your Kafka broker at `$(HOST_IP):9092`. See the [Producer example](ProducerExample.md) to learn how to connect to and use your new Kafka broker.

## SSL & authentication methods

To configure Kafka to use SSL and/or authentication methods such as SASL, see [docker-compose.yml](https://github.com/tulios/kafkajs/blob/master/docker-compose.2_4.yml). This configuration is used while developing KafkaJS, and is more complicated to set up, but may give you a more production-like development environment.