const createProducer = require('../../producer')
const createConsumer = require('../index')
const { Types } = require('../../protocol/message/compression')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
  waitForMessages,
  testIfKafka011,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    await consumer.disconnect()
    await producer.disconnect()
  })

  it('consume messages', async () => {
    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const messages = Array(100)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()

    expect(messagesConsumed[0]).toEqual({
      topic: topicName,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(messages[0].key),
        value: Buffer.from(messages[0].value),
        offset: '0',
      }),
    })

    expect(messagesConsumed[messagesConsumed.length - 1]).toEqual({
      topic: topicName,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(messages[messages.length - 1].key),
        value: Buffer.from(messages[messages.length - 1].value),
        offset: '99',
      }),
    })

    // check if all offsets are present
    expect(messagesConsumed.map(m => m.message.offset)).toEqual(messages.map((_, i) => `${i}`))
  })

  it('consume GZIP messages', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({
      acks: 1,
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
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
        batchesConsumed.push(batch)
        functionsExposed.push(resolveOffset, heartbeat, isRunning)
      },
    })

    await waitForConsumerToJoinGroup(consumer)

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ acks: 1, topic: topicName, messages: [message1, message2] })

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

    expect(functionsExposed).toEqual([
      expect.any(Function),
      expect.any(Function),
      expect.any(Function),
    ])
  })

  testIfKafka011('consume messages with 0.11 format', async () => {
    const topicName2 = `test-topic2-${secureRandom()}`
    await createTopic({ topic: topicName2 })

    cluster = createCluster({ allowExperimentalV011: true })
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.subscribe({ topic: topicName2, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const generateMessages = () =>
      Array(103)
        .fill()
        .map(() => {
          const value = secureRandom()
          return {
            key: `key-${value}`,
            value: `value-${value}`,
            headers: {
              'header-keyA': `header-valueA-${value}`,
              'header-keyB': `header-valueB-${value}`,
              'header-keyC': `header-valueC-${value}`,
            },
          }
        })

    const messages1 = generateMessages()
    const messages2 = generateMessages()

    await producer.sendBatch({
      acks: 1,
      topicMessages: [
        { topic: topicName, messages: messages1 },
        { topic: topicName2, messages: messages2 },
      ],
    })

    await waitForMessages(messagesConsumed, { number: messages1.length + messages2.length })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()

    const messagesFromTopic1 = messagesConsumed.filter(m => m.topic === topicName)
    const messagesFromTopic2 = messagesConsumed.filter(m => m.topic === topicName2)

    expect(messagesFromTopic1[0]).toEqual({
      topic: topicName,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(messages1[0].key),
        value: Buffer.from(messages1[0].value),
        headers: {
          'header-keyA': Buffer.from(messages1[0].headers['header-keyA']),
          'header-keyB': Buffer.from(messages1[0].headers['header-keyB']),
          'header-keyC': Buffer.from(messages1[0].headers['header-keyC']),
        },
        magicByte: 2,
        offset: '0',
      }),
    })

    const lastMessage1 = messages1[messages1.length - 1]
    expect(messagesFromTopic1[messagesFromTopic1.length - 1]).toEqual({
      topic: topicName,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(lastMessage1.key),
        value: Buffer.from(lastMessage1.value),
        headers: {
          'header-keyA': Buffer.from(lastMessage1.headers['header-keyA']),
          'header-keyB': Buffer.from(lastMessage1.headers['header-keyB']),
          'header-keyC': Buffer.from(lastMessage1.headers['header-keyC']),
        },
        magicByte: 2,
        offset: '102',
      }),
    })

    expect(messagesFromTopic2[0]).toEqual({
      topic: topicName2,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(messages2[0].key),
        value: Buffer.from(messages2[0].value),
        headers: {
          'header-keyA': Buffer.from(messages2[0].headers['header-keyA']),
          'header-keyB': Buffer.from(messages2[0].headers['header-keyB']),
          'header-keyC': Buffer.from(messages2[0].headers['header-keyC']),
        },
        magicByte: 2,
        offset: '0',
      }),
    })

    const lastMessage2 = messages2[messages2.length - 1]
    expect(messagesFromTopic2[messagesFromTopic2.length - 1]).toEqual({
      topic: topicName2,
      partition: 0,
      message: expect.objectContaining({
        key: Buffer.from(lastMessage2.key),
        value: Buffer.from(lastMessage2.value),
        headers: {
          'header-keyA': Buffer.from(lastMessage2.headers['header-keyA']),
          'header-keyB': Buffer.from(lastMessage2.headers['header-keyB']),
          'header-keyC': Buffer.from(lastMessage2.headers['header-keyC']),
        },
        magicByte: 2,
        offset: '102',
      }),
    })

    // check if all offsets are present
    expect(messagesFromTopic1.map(m => m.message.offset)).toEqual(messages1.map((_, i) => `${i}`))
    expect(messagesFromTopic2.map(m => m.message.offset)).toEqual(messages2.map((_, i) => `${i}`))
  })

  it('stops consuming messages when running = false', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const sleep = value => waitFor(delay => delay >= value)
    let calls = 0

    consumer.run({
      eachMessage: async event => {
        calls++
        await sleep(100)
      },
    })

    await waitForConsumerToJoinGroup(consumer)

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ acks: 1, topic: topicName, messages: [message1, message2] })
    await sleep(80) // wait for 1 message
    await consumer.disconnect() // don't give the consumer the chance to consume the 2nd message

    expect(calls).toEqual(1)
  })
})
