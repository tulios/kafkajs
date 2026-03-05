const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForMessages,
  waitForConsumerToJoinGroup,
  testIfKafkaAtLeast_4_0_0,
} = require('testHelpers')

const createProducer = require('../../producer')
const createConsumer = require('../index')

describe('Consumer > consume messages (KRaft)', () => {
  let producer, consumer, groupId, topicName, cluster

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      maxWaitTimeInMs: 100,
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  testIfKafkaAtLeast_4_0_0('produces and consumes messages on KRaft cluster', async () => {
    const messages = Array(10)
      .fill()
      .map((_, i) => ({
        key: `key-${i}`,
        value: `value-${i}`,
        headers: { 'x-kraft-test': 'true' },
      }))

    await producer.connect()
    await producer.send({ topic: topicName, messages })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const receivedMessages = []
    consumer.run({
      eachMessage: async ({ message }) => {
        receivedMessages.push({
          key: message.key.toString(),
          value: message.value.toString(),
          headers: message.headers,
        })
      },
    })

    await waitForConsumerToJoinGroup(consumer)
    await waitForMessages(receivedMessages, { number: 10 })

    expect(receivedMessages).toHaveLength(10)
    expect(receivedMessages[0].key).toBe('key-0')
    expect(receivedMessages[0].value).toBe('value-0')
    expect(receivedMessages[0].headers['x-kraft-test'].toString()).toBe('true')
  })

  testIfKafkaAtLeast_4_0_0('supports idempotent producer on KRaft cluster', async () => {
    const idempotentProducer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      maxInFlightRequests: 1,
    })

    try {
      await idempotentProducer.connect()

      const messages = Array(5)
        .fill()
        .map((_, i) => ({
          key: `idempotent-key-${i}`,
          value: `idempotent-value-${i}`,
        }))

      const result = await idempotentProducer.send({ topic: topicName, messages })
      expect(result).toBeTruthy()
      expect(result.length).toBeGreaterThan(0)
    } finally {
      await idempotentProducer.disconnect()
    }
  })
})
