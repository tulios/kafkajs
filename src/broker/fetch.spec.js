const Broker = require('./index')
const { secureRandom, createConnection } = require('testHelpers')

const minBytes = 1
const maxBytes = 1048576 // 1MB
const maxWaitTime = 5

describe('Broker > Fetch', () => {
  let topicName, seedBroker, broker

  const createMessages = (n = 0) => [
    { key: `key-${n}`, value: `some-value-${n}` },
    { key: `key-${n + 1}`, value: `some-value-${n + 1}` },
    { key: `key-${n + 2}`, value: `some-value-${n + 2}` },
  ]

  const createTopicData = (partition, messages) => [
    {
      topic: topicName,
      partitions: [
        {
          partition,
          messages,
        },
      ],
    },
  ]

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    seedBroker = new Broker(createConnection())
    await seedBroker.connect()

    const metadata = await seedBroker.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker = new Broker(createConnection(newBrokerData))
    await broker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await broker.disconnect()
    broker = new Broker(createConnection())
    await expect(broker.fetch({ topics: [] })).rejects.toEqual(new Error('Broker not connected'))
  })

  test('request', async () => {
    const targetPartition = 0
    const messages = createMessages()
    let topicData = createTopicData(targetPartition, messages)
    await broker.produce({ topicData })

    const topics = [
      {
        topic: topicName,
        partitions: [
          {
            partition: targetPartition,
            fetchOffset: 0,
            maxBytes,
          },
        ],
      },
    ]

    let fetchResponse = await broker.fetch({ maxWaitTime, minBytes, topics })
    expect(fetchResponse).toEqual({
      responses: [
        {
          topicName,
          partitions: [
            {
              errorCode: 0,
              partition: 0,
              highWatermark: '3',
              messages: [
                {
                  offset: '0',
                  size: 31,
                  crc: 120234579,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from(messages[0].key),
                  value: Buffer.from(messages[0].value),
                },
                {
                  offset: '1',
                  size: 31,
                  crc: -141862522,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from(messages[1].key),
                  value: Buffer.from(messages[1].value),
                },
                {
                  offset: '2',
                  size: 31,
                  crc: 1025004472,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from(messages[2].key),
                  value: Buffer.from(messages[2].value),
                },
              ],
            },
          ],
        },
      ],
    })

    createMessages()
    topicData = createTopicData(targetPartition, createMessages(1))
    await broker.produce({ topicData })
    fetchResponse = await broker.fetch({ maxWaitTime, minBytes, topics })
    expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
  })
})
