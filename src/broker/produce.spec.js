const Broker = require('./index')
const { Types: Compression } = require('../protocol/message/compression')
const { secureRandom, createConnection, newLogger, createTopic } = require('testHelpers')

describe('Broker > Produce', () => {
  let topicName, broker, broker2
  const timestamp = 1509928155660

  const createTopicData = () => [
    {
      topic: topicName,
      partitions: [
        {
          partition: 0,
          messages: [
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}`, timestamp },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}`, timestamp },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}`, timestamp },
          ],
        },
      ],
    },
  ]

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    broker = new Broker({
      connection: createConnection(),
      logger: newLogger(),
    })
    await broker.connect()
    createTopic({ topic: topicName })
  })

  afterEach(async () => {
    await broker.disconnect()
    broker2 && (await broker2.disconnect())
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await broker.disconnect()
    broker = new Broker({
      connection: createConnection(),
      logger: newLogger(),
    })
    await expect(broker.produce({ topicData: [] })).rejects.toEqual(
      new Error('Broker not connected')
    )
  })

  test('request', async () => {
    const metadata = await broker.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker2 = new Broker({
      connection: createConnection(newBrokerData),
      logger: newLogger(),
    })
    await broker2.connect()

    const response1 = await broker2.produce({ topicData: createTopicData() })
    expect(response1).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '0', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })

    const response2 = await broker2.produce({ topicData: createTopicData() })
    expect(response2).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '3', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })
  })

  test('request with GZIP', async () => {
    const metadata = await broker.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker2 = new Broker({
      connection: createConnection(newBrokerData),
      logger: newLogger(),
    })
    await broker2.connect()

    const response1 = await broker2.produce({
      compression: Compression.GZIP,
      topicData: createTopicData(),
    })

    expect(response1).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '0', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })

    const response2 = await broker2.produce({
      compression: Compression.GZIP,
      topicData: createTopicData(),
    })

    expect(response2).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '3', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })
  })
})
