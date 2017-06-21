const loadAPI = require('./index')
const { secureRandom, createConnection } = require('testHelpers')

describe('API > Produce', () => {
  let connection, connection2, api, topicName

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
    api = await loadAPI(connection)
  })

  afterAll(() => {
    connection.disconnect()
    connection2 && connection2.disconnect()
  })

  test('request', async () => {
    const metadata = await api.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const broker = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    connection2 = createConnection(broker)
    await connection2.connect()

    const api2 = await loadAPI(connection2)
    await api2.metadata([topicName])

    const response1 = await api2.produce({ topicData: createTopicData() })
    expect(response1).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '0', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })

    const response2 = await api2.produce({ topicData: createTopicData() })
    expect(response2).toEqual({
      topics: [
        { topicName, partitions: [{ errorCode: 0, offset: '3', partition: 0, timestamp: '-1' }] },
      ],
      throttleTime: 0,
    })
  })
})
