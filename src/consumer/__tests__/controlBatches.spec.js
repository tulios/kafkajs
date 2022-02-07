jest.setTimeout(15000)
const createProducer = require('../../producer')
const createConsumer = require('../index')
const crypto = require('crypto')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  testIfKafkaAtLeast_0_11,
  waitForMessages,
  waitFor,
  generateMessages,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, transactionalId, cluster, producer, consumer
  const maxBytes = 170

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    transactionalId = `transaction-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster({
      maxInFlightRequests: 1,
    })

    producer = createProducer({
      cluster,
      transactionalId,
      idempotent: true,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      maxBytes,
      maxBytesPerPartition: maxBytes,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  testIfKafkaAtLeast_0_11('forwards empty control batches to eachBatch', async () => {
    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break
          messagesConsumed.push(message)
          resolveOffset(message.offset)
          await heartbeat()
        }
      },
    })

    await waitForConsumerToJoinGroup(consumer)
    const messagesTransaction1 = generateMessages({ number: 20 })

    const transaction = await producer.transaction()
    for (const message of messagesTransaction1) {
      await transaction.send({ topic: topicName, messages: [message] })
    }

    await transaction.commit()
    await waitForMessages(messagesConsumed, { number: 20 })

    producer.send({ topic: topicName, messages: generateMessages({ number: 2 }) })
    await waitForMessages(messagesConsumed, { number: 22 })
  })

  testIfKafkaAtLeast_0_11('can process transactions across multiple batches', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    const endBatchProcessSpy = jest.fn()
    consumer.on(consumer.events.END_BATCH_PROCESS, endBatchProcessSpy)

    consumer.run({
      eachMessage: async () => {},
    })

    const message = {
      key: 'test',
      value: crypto.randomBytes(maxBytes),
    }

    const transaction = await producer.transaction()
    await transaction.send({
      topic: topicName,
      messages: [message, message],
    })
    await transaction.abort()

    await waitFor(
      () => endBatchProcessSpy.mock.calls.some(([event]) => event.payload.lastOffset === '2'),
      {
        delay: 50,
        maxWait: 5000,
      }
    )

    expect(endBatchProcessSpy).toHaveBeenCalledTimes(2)
  })
})
