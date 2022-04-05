---
id: version-1.10.0-transactions
title: Transactions
original_id: transactions
---

KafkaJS provides a a simple interface to support Kafka transactions.

> Note: Transactions require Kafka version >= v0.11.

## <a name="transaction-messages"></a> Sending Messages within a Transaction

You initialize a transaction by making an async call to `producer.transaction()`. The returned transaction object has the methods `send` and `sendBatch` with an identical signature to the producer. When you are done you call `transaction.commit()` or `transaction.abort()` to end the transaction. A transactionally aware consumer will only read messages which were committed.

> Note: Kafka requires that the transactional producer have the following configuration to _guarantee_ EoS ("Exactly-once-semantics"):
>
> - The producer must have a max in flight requests of 1
> - The producer must wait for acknowledgement from all replicas (acks=-1)
> - The producer must have unlimited retries

Configure the producer client with `maxInFlightRequests: 1` and `idempotent: true` to guarantee EOS. Configuring the two options will enable the settings mentioned above.

```javascript
const client = new Kafka({
  clientId: 'transactional-client',
  brokers: ['kafka1:9092', 'kafka2:9092'],
})
const producer = client.producer({ maxInFlightRequests: 1, idempotent: true })
```

Within a transaction, you can produce one or more messages. If `transaction.abort` is called, all messages will be rolled back.

```javascript
const  transaction = await producer.transaction()

try {
  await transaction.send({ topic, messages })

  await transaction.commit()
} catch (e) {
  await transaction.abort()
}
```

#### <a name="offsets"></a> Sending Offsets

To send offsets as part of a transaction, meaning they will be committed only if the transaction succeeds, use the `transaction.sendOffsets()` method. This is necessary whenever we want a transaction to produce messages derived from a consumer, in a "consume-transform-produce" loop.

```javascript
await transaction.sendOffsets({
  consumerGroupId, topics
})
```

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
