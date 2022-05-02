---
id: introduction
title: A Brief Intro to Kafka
sidebar_label: Intro to Kafka
---

Kafka is a messaging system that safely moves data between systems. Depending on how each component is
configured, it can act as a transport for real-time event tracking or as a replicated distributed database. Although it is commonly referred to as a queue, it is more accurate to say that it is something in between a queue and a database, with attributes and tradeoffs from both types of systems.

## Glossary

| Term              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cluster           | The collective group of machines that Kafka is running on                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Broker            | A single Kafka instance                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Topic             | Topics are used to organize data. You always read and write to and from a particular topic                                                                                                                                                                                                                                                                                                                                                                                   |
| Partition         | Data in a topic is spread across a number of partitions. Each partition can be thought of as a log file, ordered by time. To guarantee that you read messages in the correct order, only one member of a consumer group can read from a particular partition at a time.                                                                                                                                                                                                                        |
| Producer          | A client that writes data to one or more Kafka topics                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Consumer          | A client that reads data from one or more Kafka topics                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Replica           | Partitions are typically replicated to one or more brokers to avoid data loss.                                                                                                                                                                                                                                                                                                                                                                                               |
| Leader            | Although a partition may be replicated to one or more brokers, a single broker is elected the leader for that partition, and is the only one who is allowed to write or read to/from that partition                                                                                                                                                                                                                                                                          |
| Consumer group    | A collective group of consumer instances, identified by a [`groupId`](https://kafka.js.org/docs/consuming#a-name-options-a-options). In a horizontally scaled application, each instance would be a consumer and together they would act as a consumer group.                                                                                                                                                                                                                |
| Group Coordinator | An instance in the consumer group that is responsible for assigning partitions to consume from to the consumers in the group                                                                                                                                                                                                                                                                                                                                                 |
| Offset            | A certain point in the partition log. When a consumer has consumed a message, it "commits" that offset, meaning that it tells the broker that the consumer group has consumed that message. If the consumer group is restarted, it will restart from the highest committed offset.                                                                                                                                                                                           |
| Rebalance         | When a consumer has joined or left a consumer group (such as during booting or shutdown), the group has to "rebalance", meaning that a group coordinator has to be chosen and partitions need to be assigned to the members of the consumer group.                                                                                                                                                                                                                           |
| Heartbeat         | The mechanism by which the cluster knows which consumers are alive. Every now and then ([`heartbeatInterval`](https://kafka.js.org/docs/consuming#a-name-options-a-options)), each consumer has to send a heartbeat request to the cluster leader. If one fails to do so for a certain period ([`sessionTimeout`](https://kafka.js.org/docs/consuming#a-name-options-a-options)), it is considered dead and will be removed from the consumer group, triggering a rebalance. |


## Message Formats

Although we commonly refer to the data in topics as "messages", there is no uniform shape that messages take. From Kafka's perspective, a message is just a key-value pair, where both key and value are just sequences of bytes. It is up to the data producer and the consumers to agree on a format. Commonly you will find plain-text schemaless messages in for example JSON, or binary formats with an enforced schema such as AVRO.

### Plain-Text JSON

JSON needs no introduction. It's simple and easy to work with. The only thing we need to do is turn the message `Buffer` into a string and parse it, for example like this:

```javascript
await producer.send({
  topic,
  messages: [{
    key: 'my-key',
    value: JSON.stringify({ some: 'data' })
  }]
})

const eachMessage = async ({ /*topic, partition,*/ message }) => {
  // From Kafka's perspective, both key and value are just bytes
  // so we need to parse them.
  console.log({
    key: message.key.toString(),
    value: JSON.parse(message.value.toString())
  })

  /**
   * { key: 'my-key', value: { some: 'data' } }
   */
}
```

The downside of using JSON is that it does not enforce any kind of schema, so after you have parsed the message, you have no way of knowing what fields are available and what types they have. The data producer makes no guarantees that fields will be present or that their types won't change, making it challenging and error-prone to work with.

### AVRO

[AVRO](https://avro.apache.org/docs/current/) is a data serialization system that turns your messages into a compact binary format according to a defined schema. This allows the consumer to know exactly what each message contains, and the producer to be aware when they are making potentially breaking changes to the schema.

A schema in AVDL format looks something like this:

```
@namespace("com.kafkajs.fixtures")
protocol SimpleProto {
  record Simple {
    string foo;
  }
}
```

In order to encode or decode a message, the producer or consumer needs to have access to the correct schema. This can be read directly from a file, or fetched from a central schema registry. If so, the message contains the id of the schema used to encode it, which can be used to find the corresponding schema.

For NodeJS, this is commonly done using [confluent-schema-registry](https://www.npmjs.com/package/@kafkajs/confluent-schema-registry).
