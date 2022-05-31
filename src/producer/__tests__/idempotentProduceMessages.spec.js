jest.setTimeout(20000)

const {
  secureRandom,
  newLogger,
  createCluster,
  createTopic,
  waitForMessages,
} = require('testHelpers')
const { KafkaJSError } = require('../../errors')

const createProducer = require('../index')
const createConsumer = require('../../consumer/index')
const { describe } = require('jest-circus')

const arrayUnique = a => [...new Set(a)]

describe('Producer > Idempotent producer', () => {
  let producer, consumer, topicName, cluster, messages

  beforeAll(async () => {
    messages = Array(4)
      .fill()
      .map((_, i) => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `${i}` }
      })
  })

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    cluster = createCluster()
    producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
    })
    consumer = createConsumer({
      cluster,
      groupId: `consumer-group-id-${secureRandom()}`,
      maxWaitTimeInMs: 0,
      logger: newLogger(),
    })
    await createTopic({ topic: topicName, partitions: 1 })
    await Promise.all([producer.connect(), consumer.connect()])
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
  })

  afterEach(
    async () =>
      await Promise.all([
        producer && (await producer.disconnect()),
        consumer && (await consumer.disconnect()),
      ])
  )

  it('sequential produce() calls > all messages are written to the partition once, in order', async () => {
    const messagesConsumed = []

    for (const m of messages) {
      await producer.send({ acks: -1, topic: topicName, messages: [m] })
    }

    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    messagesConsumed.forEach(({ message: { value } }, i) =>
      expect(value.toString()).toEqual(`${i}`)
    )
  })

  it('sequential produce() calls > where produce() throws a retriable error, all messages are written to the partition once, in order', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })

      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce(() => {
        throw new KafkaJSError('retriable error')
      })
    }

    const messagesConsumed = []

    for (const m of messages) {
      await producer.send({ acks: -1, topic: topicName, messages: [m] })
    }

    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    messagesConsumed.forEach(({ message: { value } }, i) =>
      expect(value.toString()).toEqual(`${i}`)
    )
  })

  it('sequential produce() calls > where produce() throws a retriable error after the message is written to the log, all messages are written to the partition once, in order', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })
      const originalCall = broker.produce.bind(broker)
      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce()
      brokerProduce.mockImplementationOnce()
      brokerProduce.mockImplementationOnce(async (...args) => {
        await originalCall(...args)
        throw new KafkaJSError('retriable error')
      })
    }

    const messagesConsumed = []

    for (const m of messages) {
      await producer.send({ acks: -1, topic: topicName, messages: [m] })
    }

    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    messagesConsumed.forEach(({ message: { value } }, i) =>
      expect(value.toString()).toEqual(`${i}`)
    )
  })

  it('concurrent produce() calls > all messages are written to the partition once', async () => {
    const messagesConsumed = []

    await Promise.all(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    )

    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })
    expect(messagesConsumed).toHaveLength(messages.length)
  })

  it('concurrent produce() calls > where produce() throws a retriable error on the first call, all messages are written to the partition once', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })

      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce(async () => {
        throw new KafkaJSError('retriable error')
      })
    }

    await Promise.allSettled(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    )

    const messagesConsumed = []
    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(arrayUnique(messagesConsumed.map(({ message: { value } }) => value))).toHaveLength(
      messages.length
    )
  })

  it('concurrent produce() calls > where produce() throws a retriable error on 2nd call, all messages are written to the partition once', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })

      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce()
      brokerProduce.mockImplementationOnce(async () => {
        throw new KafkaJSError('retriable error')
      })
    }

    await Promise.allSettled(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    )

    const messagesConsumed = []
    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(arrayUnique(messagesConsumed.map(({ message: { value } }) => value))).toHaveLength(
      messages.length
    )
  })

  it('concurrent produce() calls > where produce() throws a retriable error after the message is written to the log, all messages are written to the partition once', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })
      const originalCall = broker.produce.bind(broker)
      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce(async (...args) => {
        await originalCall(...args)
        throw new KafkaJSError('retriable error')
      })
    }

    const messagesConsumed = []

    await Promise.all(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    )

    await consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(arrayUnique(messagesConsumed.map(({ message: { value } }) => value))).toHaveLength(
      messages.length
    )
  })
})
