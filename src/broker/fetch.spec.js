const Broker = require('./index')
const {
  secureRandom,
  createConnection,
  newLogger,
  createTopic,
  retryProtocol,
} = require('testHelpers')
const { Types: Compression } = require('../protocol/message/compression')

const minBytes = 1
const maxBytes = 10485760 // 10MB
const maxBytesPerPartition = 1048576 // 1MB
const maxWaitTime = 5
const timestamp = 1509827900073

describe('Broker > Fetch', () => {
  let topicName, seedBroker, broker

  const createMessages = (n = 0) => [
    { key: `key-${n}`, value: `some-value-${n}`, timestamp },
    { key: `key-${n + 1}`, value: `some-value-${n + 1}`, timestamp },
    { key: `key-${n + 2}`, value: `some-value-${n + 2}`, timestamp },
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
    seedBroker = new Broker({
      connection: createConnection(),
      logger: newLogger(),
    })
    await seedBroker.connect()
    createTopic({ topic: topicName })

    const metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await seedBroker.metadata([topicName])
    )

    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker = new Broker({
      connection: createConnection(newBrokerData),
      logger: newLogger(),
    })
    await broker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await broker.disconnect()
    broker = new Broker({ connection: createConnection(), logger: newLogger() })
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
            maxBytes: maxBytesPerPartition,
          },
        ],
      },
    ]

    let fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse).toEqual({
      throttleTime: 0,
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
                  size: 39,
                  crc: 924859032,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
                  key: Buffer.from(messages[0].key),
                  value: Buffer.from(messages[0].value),
                },
                {
                  offset: '1',
                  size: 39,
                  crc: -947797683,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
                  key: Buffer.from(messages[1].key),
                  value: Buffer.from(messages[1].value),
                },
                {
                  offset: '2',
                  size: 39,
                  crc: 219335539,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
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
    fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
  })

  test('request with GZIP', async () => {
    const targetPartition = 0
    const messages = createMessages()
    let topicData = createTopicData(targetPartition, messages)
    await broker.produce({ topicData, compression: Compression.GZIP })

    const topics = [
      {
        topic: topicName,
        partitions: [
          {
            partition: targetPartition,
            fetchOffset: 0,
            maxBytes: maxBytesPerPartition,
          },
        ],
      },
    ]

    let fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse).toEqual({
      throttleTime: 0,
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
                  size: 39,
                  crc: 924859032,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
                  key: Buffer.from(messages[0].key),
                  value: Buffer.from(messages[0].value),
                },
                {
                  offset: '1',
                  size: 39,
                  crc: -947797683,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
                  key: Buffer.from(messages[1].key),
                  value: Buffer.from(messages[1].value),
                },
                {
                  offset: '2',
                  size: 39,
                  crc: 219335539,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827900073',
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
    await broker.produce({ topicData, compression: Compression.GZIP })
    fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
  })
})
