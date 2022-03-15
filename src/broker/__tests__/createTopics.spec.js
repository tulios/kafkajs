const {
  createCluster,
  createConnectionPool,
  connectionOpts,
  secureRandom,
  newLogger,
} = require('testHelpers')
const { ConfigResourceTypes } = require('../../..')
const createAdmin = require('../../admin')

const Broker = require('../index')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > createTopics', () => {
  let seedBroker, broker, admin

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

    admin = createAdmin({ logger: newLogger(), cluster: createCluster() })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
    admin && (await admin.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topicErrors: [
        {
          topic: topicName1,
          errorCode: 0,
          errorMessage: null,
        },
        {
          topic: topicName2,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })

  test('request with validateOnly', async () => {
    await broker.connect()
    const topicName = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName }],
      validateOnly: true,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topicErrors: [
        {
          topic: topicName,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })

  it('should use the default replication factor and num partitions if not specified', async () => {
    await broker.connect()
    const topicName = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName }],
    })

    expect(response).toEqual(
      expect.objectContaining({
        topicErrors: expect.arrayContaining([
          expect.objectContaining({
            topic: topicName,
            errorCode: 0,
          }),
        ]),
      })
    )

    await admin.connect()
    const describeResponse = await admin.describeConfigs({
      resources: [
        {
          type: ConfigResourceTypes.BROKER,
          name: '0',
          configNames: ['default.replication.factor', 'num.partitions'],
        },
      ],
    })
    const defaultReplicationFactor = parseInt(
      describeResponse.resources[0].configEntries[0].configValue,
      null,
      10
    )
    const defaultNumPartitions = parseInt(
      describeResponse.resources[0].configEntries[1].configValue,
      null,
      10
    )
    expect(defaultReplicationFactor).toBeGreaterThan(0)
    expect(defaultNumPartitions).toBeGreaterThan(0)

    const metadata = await broker.metadata([topicName])
    expect(metadata.topicMetadata[0].partitionMetadata[0].replicas.length).toEqual(
      defaultReplicationFactor
    )
    expect(metadata.topicMetadata[0].partitionMetadata).toHaveLength(defaultNumPartitions)
  })
})
