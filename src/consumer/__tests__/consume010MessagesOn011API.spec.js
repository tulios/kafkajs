const createProducer = require('../../producer')
const createConsumer = require('../index')
const { Types } = require('../../protocol/message/compression')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
  testIfKafka_0_11,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

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

describe('Consumer', () => {
  let topicName, groupId, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({
      topic: topicName,
      partitions: 2,
      config: [{ name: 'message.format.version', value: '0.10.0' }],
    })

    producer = createProducer({
      cluster: createCluster({ allowExperimentalV011: false }),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      groupId,
      cluster: createCluster({ allowExperimentalV011: true }),
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  testIfKafka_0_11('consume 0.10 messages with 0.11 API', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messages = generateMessages()
    await producer.send({
      acks: 1,
      topic: topicName,
      messages,
    })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)
    await waitForMessages(messagesConsumed, { number: messages.length })
  })

  testIfKafka_0_11('consume 0.10 GZIP messages with 0.11 API', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messages = generateMessages()
    await producer.send({
      acks: 1,
      topic: topicName,
      compression: Types.GZIP,
      messages,
    })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)
    await waitForMessages(messagesConsumed, { number: messages.length })
  })
})
