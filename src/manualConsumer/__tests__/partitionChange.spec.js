const createProducer = require('../../producer')
const createManualConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
  addPartitions,
} = require('testHelpers')

describe('ManualConsumer', () => {
  let topicName, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    await createTopic({ topic: topicName })

    producer = createProducer({
      cluster: createCluster({ metadataMaxAge: 20 }),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createManualConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  test('consumes newly added partitions after metadata update', async () => {
    await consumer.connect()
    await producer.connect()

    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

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
