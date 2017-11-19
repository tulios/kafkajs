const createProducer = require('../producer')
const createConsumer = require('./index')
const { Types } = require('../protocol/message/compression')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  sslConnectionOpts,
  sslBrokers,
  saslBrokers,
  waitFor,
  waitForMessages,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    await consumer.disconnect()
    await producer.disconnect()
  })

  test('support SSL connections', async () => {
    cluster = createCluster(sslConnectionOpts(), sslBrokers())
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })

    await consumer.connect()
  })

  test('support SASL PLAIN connections', async () => {
    cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'plain',
          username: 'test',
          password: 'testtest',
        },
      }),
      saslBrokers()
    )

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })

    await consumer.connect()
  })

  test('reconnects the cluster if disconnected', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName })
    await consumer.run({ eachMessage: async () => {} })

    expect(cluster.isConnected()).toEqual(true)
    await cluster.disconnect()
    expect(cluster.isConnected()).toEqual(false)

    await expect(waitFor(() => cluster.isConnected())).resolves.toBeTruthy()
  })

  it('consume messages', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    // Make two separate calls
    await producer.send({ topic: topicName, messages: [message1] })
    await producer.send({ topic: topicName, messages: [message2] })

    await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      },
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message2.key),
          value: Buffer.from(message2.value),
          offset: '1',
        }),
      },
    ])
  })

  it('consume GZIP messages', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({
      topic: topicName,
      compression: Types.GZIP,
      messages: [message1, message2],
    })

    await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      },
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message2.key),
          value: Buffer.from(message2.value),
          offset: '1',
        }),
      },
    ])
  })

  it('consume batches', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const batchesConsumed = []
    consumer.run({ eachBatch: async ({ batch }) => batchesConsumed.push(batch) })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ topic: topicName, messages: [message1, message2] })

    await expect(waitForMessages(batchesConsumed)).resolves.toEqual([
      {
        topic: topicName,
        partition: 0,
        highWatermark: '2',
        messages: [
          expect.objectContaining({
            key: Buffer.from(message1.key),
            value: Buffer.from(message1.value),
            offset: '0',
          }),
          expect.objectContaining({
            key: Buffer.from(message2.key),
            value: Buffer.from(message2.value),
            offset: '1',
          }),
        ],
      },
    ])
  })
})
