const Broker = require('./index')
const { Types: Compression } = require('../protocol/message/compression')
const { secureRandom, createConnection } = require('testHelpers')

describe('Broker > Produce', () => {
  let topicName, broker, broker2

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

  beforeAll(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  test('request', async () => {
    try {
      broker = new Broker(createConnection())
      await broker.connect()

      const metadata = await broker.metadata([topicName])
      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker(createConnection(newBrokerData))
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
    } finally {
      await broker.disconnect()
      broker2 && (await broker2.disconnect())
    }
  })

  test('request with GZIP', async () => {
    try {
      broker = new Broker(createConnection())
      await broker.connect()

      const metadata = await broker.metadata([topicName])
      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker(createConnection(newBrokerData))
      await broker2.connect()

      const response1 = await broker2.produce({
        compression: Compression.GZIP,
        topicData: createTopicData(),
      })

      expect(response1).toEqual({
        topics: [
          { topicName, partitions: [{ errorCode: 0, offset: '6', partition: 0, timestamp: '-1' }] },
        ],
        throttleTime: 0,
      })
    } finally {
      await broker.disconnect()
      broker2 && (await broker2.disconnect())
    }
  })
})
