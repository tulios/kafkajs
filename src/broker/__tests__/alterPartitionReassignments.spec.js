const { createConnectionPool, connectionOpts, secureRandom, newLogger } = require('testHelpers')

const Broker = require('../index')

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

    await broker.createTopics({
      topics: [
        {
          topic: topicName1,
          replicaAssignment: [
            { partition: 0, replicas: [2, 1] },
            { partition: 1, replicas: [0, 2] },
          ],
        },
      ],
    })

    const response = await broker.alterPartitionReassignments({
      topics: [
        {
          topic: topicName1,
          partitionAssignment: [
            { partition: 0, replicas: [1, 0] },
            { partition: 1, replicas: [2, 1] },
          ],
        },
      ],
      timeout: 5000,
    })

    response.responses[0].partitions.sort((a, b) => b.partition - a.partition)

    expect(response).toEqual({
      throttleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: topicName1,
          partitions: [
            { partition: 1, errorCode: 0 },
            { partition: 0, errorCode: 0 },
          ],
        },
      ],
    })
    await broker.deleteTopics({ topics: [topicName1] })
  })
})
