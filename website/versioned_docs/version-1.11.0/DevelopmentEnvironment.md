---
id: version-1.11.0-development-environment
title: Development Environment
original_id: development-environment
---

When developing KafkaJS, we run a Kafka cluster in a similar way to what is described in [Running Kafka in Development](DockerLocal.md), using [`docker`](https://docs.docker.com/) and [`docker-compose`](https://docs.docker.com/compose/install/). Before you proceed, make sure that you have both `docker` and `docker-compose` available.

## Running Kafka

For testing KafkaJS we use a multi-broker Kafka cluster as well as Zookeeper for authentication. To start the cluster and generate credentials, run the following from the root of the repository:

```sh
# This will run a Kafka cluster configured with your current IP
./scripts/dockerComposeUp.sh
./scripts/createScramCredentials.sh
```

This boots the Kafka cluster using the default docker-compose.yml file described in [scripts/dockerComposeUp.sh](https://github.com/tulios/kafkajs/blob/master/scripts/dockerComposeUp.sh). If you want to run a different version of Kafka, specify a different compose file using the `COMPOSE_FILE` environment variable:

```sh
COMPOSE_FILE="docker-compose.2_2.yml" ./scripts/dockerComposeUp.sh
```

If you run `docker-compose -f docker-compose.2_2.yml ps` (specify whichever compose file you used in the step above), you should see something like:

```sh
$ docker-compose -f docker-compose.2_2.yml ps
WARNING: The HOST_IP variable is not set. Defaulting to a blank string.
      Name                    Command               State                                   Ports
----------------------------------------------------------------------------------------------------------------------------------
kafkajs_kafka1_1   start-kafka.sh                   Up      0.0.0.0:9092->9092/tcp, 0.0.0.0:9093->9093/tcp, 0.0.0.0:9094->9094/tcp
kafkajs_kafka2_1   start-kafka.sh                   Up      0.0.0.0:9095->9095/tcp, 0.0.0.0:9096->9096/tcp, 0.0.0.0:9097->9097/tcp
kafkajs_kafka3_1   start-kafka.sh                   Up      0.0.0.0:9098->9098/tcp, 0.0.0.0:9099->9099/tcp, 0.0.0.0:9100->9100/tcp
kafkajs_zk_1       /bin/sh -c /usr/sbin/sshd  ...   Up      0.0.0.0:2181->2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
```

The user credentials are listed in [scripts/createScramCredentials.sh](https://github.com/tulios/kafkajs/blob/master/scripts/createScramCredentials.sh).

You should now be able to connect to your cluster as such:

```javascript
const fs = require('fs')
const ip = require('ip')

const { Kafka, CompressionTypes, logLevel } = require('./index')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-producer',
  ssl: {
    servername: 'localhost',
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
})
```
