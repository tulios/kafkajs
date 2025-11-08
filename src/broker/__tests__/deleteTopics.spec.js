const {
  createConnectionPool,
  connectionOpts,
  secureRandom,
  newLogger,
  createCluster,
} = require('testHelpers')

const Broker = require('../index')
const createProducer = require('../../producer')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > deleteTopics', () => {
  let seedBroker, broker

  beforeEach(async () => {
    seedBroker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find(b => b.nodeId === metadata.controllerId)

    broker = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`

    await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    const response = await broker.deleteTopics({
      topics: [topicName1, topicName2],
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topicErrors: [
        { topic: topicName1, errorCode: 0 },
        { topic: topicName2, errorCode: 0 },
      ].sort(topicNameComparator),
    })
  })

  test('post deletion a topic, message should be published to other targeted topics with Publisher API', async () => {
    await broker.connect()
    const cluster = createCluster({ allowAutoTopicCreation: false })
    const producer = createProducer({
      cluster,
      logger: newLogger(),
    })
    try {
      const topic1 = `test-topic-${secureRandom()}`
      const topic2 = `test-topic-${secureRandom()}`

      // Create both topics
      await broker.createTopics({
        topics: [{ topic: topic1 }, { topic: topic2 }],
      })

      await producer.connect()

      await producer.send({
        topic: topic1,
        messages: [{ key: 'key1', value: 'value1' }],
      })

      await producer.send({
        topic: topic2,
        messages: [{ key: 'key2', value: 'value2' }],
      })

      // Delete topic1
      const deleteResponse = await broker.deleteTopics({
        topics: [topic1],
      })

      expect(deleteResponse.topicErrors).toEqual(
        expect.arrayContaining([{ topic: topic1, errorCode: 0 }])
      )
      await expect(
        producer.send({
          topic: topic2,
          messages: [{ key: 'key3', value: 'value3' }],
        })
      ).toResolve()
    } catch (error) {
      console.error('error ', error)
    } finally {
      await producer.disconnect()
      await broker.disconnect()
    }
  })
})
