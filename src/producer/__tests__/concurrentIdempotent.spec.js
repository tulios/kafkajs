const {
  secureRandom,
  newLogger,
  createCluster,
  testIfKafkaAtLeast_0_11,
  createTopic,
  waitForMessages,
} = require('testHelpers')
const createProducer = require('../index')
const createConsumer = require('../../consumer/index')

describe('Producer > Idempotent producer', () => {
  let producer, consumer, topicName, transactionalId

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    const cluster = createCluster()
    producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    })
    consumer = createConsumer({
      cluster,
      groupId: `consumer-group-id-${secureRandom()}`,
      maxWaitTimeInMs: 0,
      logger: newLogger(),
    })
    await createTopic({ topic: topicName, partitions: 3 })
    await Promise.all([producer.connect(), consumer.connect()])
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    consumer && (await consumer.disconnect())
  })

  testIfKafkaAtLeast_0_11(
    'Concurrent calls to produce(), all messages are accepted by the broker',
    async () => {
      const messagesConsumed = []

      const messages = Array(30)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await Promise.all(
        messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
      )
      await Promise.all(
        messages.map(m => producer.send({ acks: -1, topic: topicName, messages: [m] }))
      )

      consumer.run({ eachMessage: async message => messagesConsumed.push(message) })

      await waitForMessages(messagesConsumed, { number: messages.length * 2 })
    }
  )
})
