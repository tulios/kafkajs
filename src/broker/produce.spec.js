const Broker = require('./index')
const loadApiVersions = require('./apiVersions')
const { secureRandom, createConnection } = require('testHelpers')

describe('Broker > Produce', () => {
  let connection, connection2, broker, topicName, versions

  const createTopicData = () => [
    {
      topic: topicName,
      partitions: [
        {
          partition: 0,
          messages: [
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
          ],
        },
      ],
    },
  ]

  beforeAll(async () => {
    topicName = `test-topic-${secureRandom()}`
    connection = createConnection()
    await connection.connect()
    versions = await loadApiVersions(connection)
    broker = new Broker(connection, versions)
  })

  afterAll(() => {
    connection.disconnect()
    connection2 && connection2.disconnect()
  })

  test('request', async () => {
    const metadata = await broker.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    connection2 = createConnection(newBrokerData)
    await connection2.connect()

    const broker2 = new Broker(connection2, versions)

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
})
