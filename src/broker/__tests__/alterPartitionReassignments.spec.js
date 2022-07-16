const { createConnectionPool, connectionOpts, secureRandom, newLogger } = require('testHelpers')

const Broker = require('../index')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > alterPartitionReassignments', () => {
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
      allowAutoTopicCreation: false,
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

    console.log('created Topic')

    const response = await broker.alterPartitionReassignments({
      topics: [
        {
          topic: topicName1,
          partitionAssignment: [{ partition: 0, replicas: [0, 2] }],
        },
        {
          topic: topicName2,
          partitionAssignment: [{ partition: 0, replicas: [0, 1] }],
        },
      ],
      timeout: 5000,
    })

    expect(response).toEqual({
      throttleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: topicName1,
          partitions: [{ partition: 0, errorCode: 0 }],
        },
        {
          topic: topicName2,
          partitions: [{ partition: 0, errorCode: 0 }],
        },
      ].sort(topicNameComparator),
    })
  })
})
