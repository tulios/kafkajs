const createProducer = require('../producer')
const createConsumer = require('./index')
const { Types } = require('../protocol/message/compression')
const { KafkaJSError } = require('../errors')

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
      heartbeatInterval: 100,
    })
  })

  afterEach(async () => {
    await consumer.disconnect()
    await producer.disconnect()
  })

  test('on throws an error when provided with an invalid event name', () => {
    expect(() => consumer.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      KafkaJSError,
      /Event name should be one of/
    )
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
    const functionsExposed = []
    consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        batchesConsumed.push(batch)
        functionsExposed.push(resolveOffset, heartbeat)
      },
    })

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

    expect(functionsExposed).toEqual([expect.any(Function), expect.any(Function)])
  })

  it('stops consuming messages when running = false', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const sleep = value => waitFor(delay => delay >= value)
    let calls = 0

    await consumer.run({
      eachMessage: async event => {
        calls++
        await sleep(100)
      },
    })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ topic: topicName, messages: [message1, message2] })
    await sleep(80) // wait for 1 message
    await consumer.disconnect() // don't give the consumer the chance to consume the 2nd message

    expect(calls).toEqual(1)
  })

  it('recovers from offset out of range', async () => {
    await consumer.connect()
    await producer.connect()

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const { generationId, memberId } = await coordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
    })

    const groupAssignment = [
      {
        memberId,
        memberAssignment: { [topicName]: [0] },
      },
    ]

    await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment,
    })

    const topics = [
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: '11' }],
      },
    ]

    await coordinator.offsetCommit({
      groupId,
      groupGenerationId: generationId,
      memberId,
      topics,
    })

    await coordinator.leaveGroup({ groupId, memberId })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    await producer.send({ topic: topicName, messages: [message1] })

    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

    await expect(waitForMessages(messagesConsumed)).resolves.toEqual([
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      },
    ])
  })

  describe('when eachMessage throws an error', () => {
    it('commits the previous offsets', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      const key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ topic: topicName, messages: [message1, message2, message3] })

      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      let raisedError = false
      await consumer.run({
        eachMessage: async event => {
          if (event.message.key.toString() === `key-${key3}`) {
            raisedError = true
            throw new Error('some error')
          }
        },
      })

      await expect(waitFor(() => raisedError)).resolves.toBeTruthy()
      const coordinator = await cluster.findGroupCoordinator({ groupId })
      const offsets = await coordinator.offsetFetch({
        groupId,
        topics: [
          {
            topic: topicName,
            partitions: [{ partition: 0 }],
          },
        ],
      })

      expect(offsets).toEqual({
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '2', partition: 0 }],
            topic: topicName,
          },
        ],
      })
    })
  })

  describe('when eachBatch throws an error', () => {
    it('commits the previous offsets', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      const key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ topic: topicName, messages: [message1, message2, message3] })

      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      let raisedError = false
      await consumer.run({
        eachBatch: async ({ batch, resolveOffset }) => {
          for (let message of batch.messages) {
            if (message.key.toString() === `key-${key3}`) {
              raisedError = true
              throw new Error('some error')
            }
            resolveOffset(message.offset)
          }
        },
      })

      await expect(waitFor(() => raisedError)).resolves.toBeTruthy()
      const coordinator = await cluster.findGroupCoordinator({ groupId })
      const offsets = await coordinator.offsetFetch({
        groupId,
        topics: [
          {
            topic: topicName,
            partitions: [{ partition: 0 }],
          },
        ],
      })

      expect(offsets).toEqual({
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '2', partition: 0 }],
            topic: topicName,
          },
        ],
      })
    })
  })
})
