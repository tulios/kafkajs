---
id: version-3.0.0-transactions
title: Transactions
original_id: transactions
---

KafkaJS provides a simple interface to support Kafka transactions.

> Note: Transactions require Kafka version >= v0.11.

## <a name="transaction-messages"></a> Sending Messages within a Transaction

You initialize a transaction by making an async call to `producer.transaction()`. The returned transaction object has the methods `send` and `sendBatch` with an identical signature to the producer. When you are done you call `transaction.commit()` or `transaction.abort()` to end the transaction. A transactionally aware consumer will only read messages which were committed.

> Note: Kafka requires that the transactional producer have the following configuration to _guarantee_ EoS ("Exactly-once-semantics"):
>
> - The producer must have a max in flight requests of 1
> - The producer must wait for acknowledgement from all replicas (acks=-1)
> - The producer must have unlimited retries

Configure the producer client with `maxInFlightRequests: 1`, `idempotent: true` and a `transactionalId` to guarantee EOS. Configuring the options will enable the settings mentioned above.

```javascript
const client = new Kafka({
  clientId: 'transactional-client',
  brokers: ['kafka1:9092', 'kafka2:9092'],
})
const producer = client.producer({
  transactionalId: 'my-transactional-producer',
  maxInFlightRequests: 1,
  idempotent: true
})
```

### Producer Configuration for Transactions

| option              | description                                                                                                      | default              | type      | required |
| ------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------- | --------- | -------- |
| transactionalId     | Unique identifier for the transactional producer. Enables Kafka to fence out zombie instances                    | `undefined`          | `String`  | **Yes** (for transactions) |
| idempotent          | Ensures each message is written exactly once. Required for EOS                                                   | `false`              | `Boolean` | **Yes** (for EOS) |
| maxInFlightRequests | Max concurrent requests. Must be `1` for EOS guarantees                                                          | `null` (no limit)    | `Number`  | **Yes** (for EOS) |
| transactionTimeout  | Max time in ms the coordinator waits for a status update before aborting                                          | `60000`              | `Number`  | No |

Within a transaction, you can produce one or more messages. If `transaction.abort` is called, all messages will be rolled back.

```javascript
const transaction = await producer.transaction()

try {
  await transaction.send({ topic, messages })

  await transaction.commit()
} catch (e) {
  await transaction.abort()
}
```

### Transaction Object API

The transaction object returned by `producer.transaction()` provides:

| method         | description                                                         | return type       |
| -------------- | ------------------------------------------------------------------- | ----------------- |
| `send(record)` | Send messages within the transaction (same signature as `producer.send`) | `Promise<RecordMetadata[]>` |
| `sendBatch(batch)` | Send batch within the transaction (same signature as `producer.sendBatch`) | `Promise<RecordMetadata[]>` |
| `sendOffsets(offsets)` | Commit consumer offsets as part of the transaction. See [Sending Offsets](#offsets) | `Promise<void>` |
| `commit()`     | Commit the transaction                                               | `Promise<void>`   |
| `abort()`      | Abort the transaction, rolling back all messages                     | `Promise<void>`   |
| `isActive()`   | Returns whether the transaction is still active                      | `Boolean`         |

### Choosing a `transactionalId`

The `transactionalId` allows Kafka to fence out zombie instances by rejecting writes from producers with the same `transactionalId`, allowing only writes from the most recently registered producer. To ensure EoS semantics in a stream processing application, it is important that the `transactionalId` is always the same for a given input topic and partition in the read-process-write cycle.

The simplest way to achieve this is to encode the topic and partition in the `transactionalId` itself such as the scheme: `"myapp-producer-" + topic + "-" + partition`.

[This article from Confluent](https://www.confluent.io/blog/transactions-apache-kafka/) goes into much greater detail on how transactions work and is a recommended read before deciding on a `transactionalId`.

## <a name="offsets"></a> Sending Offsets

To send offsets as part of a transaction, meaning they will be committed only if the transaction succeeds, use the `transaction.sendOffsets()` method. This is necessary whenever we want a transaction to produce messages derived from a consumer, in a "consume-transform-produce" loop.

```javascript
await transaction.sendOffsets({
  consumerGroupId, topics
})
```

| property        | description                                   | type     | required |
| --------------- | --------------------------------------------- | -------- | -------- |
| consumerGroupId | The consumer group ID to commit offsets for    | `String` | **Yes** |
| topics          | Array of topic-partition offsets to commit      | `Array`  | **Yes** |

`topics` has the following structure:

```javascript
[{
  topic: <String>,
  partitions: [{
    partition: <Number>,
    offset: <String>
  }]
}]
```

### Complete Consume-Transform-Produce Example

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })

const consumer = kafka.consumer({ groupId: 'my-group' })
const producer = kafka.producer({
  transactionalId: 'my-txn-producer',
  maxInFlightRequests: 1,
  idempotent: true,
})

await consumer.connect()
await producer.connect()
await consumer.subscribe({ topics: ['input-topic'] })

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    const transaction = await producer.transaction()

    try {
      // Transform and produce
      const transformedValue = transform(message.value)
      await transaction.send({
        topic: 'output-topic',
        messages: [{ key: message.key, value: transformedValue }],
      })

      // Commit consumer offsets as part of the transaction
      await transaction.sendOffsets({
        consumerGroupId: 'my-group',
        topics: [{
          topic,
          partitions: [{ partition, offset: (Number(message.offset) + 1).toString() }],
        }],
      })

      await transaction.commit()
    } catch (e) {
      await transaction.abort()
    }
  },
})
```

## <a name="transactional-consumer"></a> Transactional Consumer

To only read committed messages (skip messages from aborted transactions), configure the consumer with `readUncommitted: false` (this is the default):

```javascript
const consumer = kafka.consumer({
  groupId: 'my-group',
  readUncommitted: false, // default - only reads committed messages
})
```

| option          | description                                                              | default | type      |
| --------------- | ------------------------------------------------------------------------ | ------- | --------- |
| readUncommitted | If `true`, consumer reads all messages including uncommitted/aborted ones | `false` | `Boolean` |
