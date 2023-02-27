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
    const topicName = `test-topic-${secureRandom()}`

    await broker.createTopics({
      topics: [
        {
          topic: topicName,
          replicaAssignment: [{ partition: 0, replicas: [0, 2] }],
        },
      ],
    })

    await broker.alterPartitionReassignments({
      topics: [
        {
          topic: topicName,
          partitionAssignment: [{ partition: 0, replicas: [2, 1] }],
        },
      ],
    })

    const response = await broker.listPartitionReassignments({
      topics: [
        {
          topic: topicName,
          partitions: [0],
        },
      ],
    })

    response.topics[0].partitions[0].replicas.sort()
    expect(response).toEqual({
      throttleTime: 0,
      errorCode: 0,
      topics: [
        {
          name: topicName,
          partitions: [
            { partition: 0, replicas: [0, 1, 2], addingReplicas: [1], removingReplicas: [0] },
          ],
        },
      ],
    })

    await broker.deleteTopics({ topics: [topicName] })
  })
})
