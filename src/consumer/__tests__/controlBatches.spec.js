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
      maxBytes: 170,
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

  it('can process transactions across multiple batches', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    let offset = '0'
    consumer.on(consumer.events.END_BATCH_PROCESS, event => {
      console.log(event.payload.firstOffset)
      offset = event.payload.lastOffset
    })

    consumer.run({
      eachMessage: async payload => {},
    })

    const range = [1, 2]
    const message = {
      key: 'test',
      // half size of consumer maxBytes
      value: crypto.randomBytes(130),
    }

    const transaction = await producer.transaction()
    await transaction.send({
      topic: topicName,
      acks: -1,
      messages: range.map(() => message),
    })
    await transaction.abort()

    const done = waitFor(() => offset === '2', {
      delay: 50,
      maxWait: 5000,
    })

    await expect(done).resolves.toBe(true)
  })
})
