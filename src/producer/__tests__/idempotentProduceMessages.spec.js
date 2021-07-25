jest.setTimeout(20000)

const PromiseAllSettled = require('../../utils/promiseAllSettled')

const {
  secureRandom,
  newLogger,
  createCluster,
  createTopic,
  waitForMessages,
} = require('testHelpers')
const { KafkaJSError, KafkaJSProtocolError } = require('../../errors')

const createProducer = require('../index')
const createConsumer = require('../../consumer/index')
const { describe } = require('jest-circus')
const sleep = require('../../utils/sleep')

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
      brokerProduce.mockImplementationOnce(async () => {
        await sleep(5)
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

    expect(arrayUnique(messagesConsumed.map(({ message: { value } }) => value))).toHaveLength(
      messages.length
    )
  })

  it('concurrent produce() calls > where produce() throws a retriable error on the first call, all subsequent calls throw UNKNOWN_PRODUCER_ID', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })

      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce(async () => {
        await sleep(100)
        throw new KafkaJSError('retriable error')
      })
    }

    const settlements = await PromiseAllSettled(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    ).catch(e => e)

    settlements
      .filter(({ status }) => status === 'rejected')
      .forEach(({ reason }) => {
        expect(reason).toBeInstanceOf(KafkaJSProtocolError)
        expect(reason.type).toBe('UNKNOWN_PRODUCER_ID')
      })

    expect(settlements.filter(({ status }) => status === 'fulfilled')).toHaveLength(1)
  })

  it('concurrent produce() calls > where produce() throws a retriable error on 2nd call, all subsequent calls throw OUT_OF_ORDER_SEQUENCE_NUMBER', async () => {
    for (const nodeId of [0, 1, 2]) {
      const broker = await cluster.findBroker({ nodeId })

      const brokerProduce = jest.spyOn(broker, 'produce')
      brokerProduce.mockImplementationOnce()
      brokerProduce.mockImplementationOnce(async () => {
        await sleep(1)
        throw new KafkaJSError('retriable error')
      })
    }

    const settlements = await PromiseAllSettled(
      messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
    ).catch(e => e)

    settlements
      .filter(({ status }) => status === 'rejected')
      .forEach(({ reason }) => {
        expect(reason).toBeInstanceOf(KafkaJSProtocolError)
        expect(reason.type).toBe('OUT_OF_ORDER_SEQUENCE_NUMBER')
      })

    expect(settlements.filter(({ status }) => status === 'fulfilled')).toHaveLength(2)
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
