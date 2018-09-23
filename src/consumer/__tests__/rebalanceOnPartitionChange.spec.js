const createProducer = require('../../producer')
const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
  waitForConsumerToJoinGroup,
  addPartitions,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, producer, consumer1, consumer2

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    producer = createProducer({
      cluster: createCluster({ metadataMaxAge: 20 }),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer1 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 3000 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
    producer && (await producer.disconnect())
  })

  test('trigger rebalance if partitions change after metadata update', async () => {
    await consumer1.connect()
    await consumer2.connect()
    await producer.connect()

    await consumer1.subscribe({ topic: topicName, fromBeginning: true })
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer1.run({ eachMessage: async event => messagesConsumed.push(event) })
    consumer2.run({ eachMessage: async event => messagesConsumed.push(event) })

    await Promise.all([
      waitForConsumerToJoinGroup(consumer1),
      waitForConsumerToJoinGroup(consumer2),
    ])

    let messages = Array(2)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: 2 })

    await addPartitions({ topic: topicName, partitions: 4 })

    messages = Array(6)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: 8 })
  })
})
