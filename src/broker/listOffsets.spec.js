const Broker = require('./index')
const { secureRandom, createConnection, newLogger, createTopic } = require('testHelpers')

const EARLIEST = -2
const LATEST = -1

describe('Broker > ListOffsets', () => {
  let topicName, seedBroker, broker

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    seedBroker = new Broker(createConnection(), newLogger())
    await seedBroker.connect()
    await createTopic(seedBroker, topicName)

    const metadata = await seedBroker.metadata([topicName])
    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker = new Broker(createConnection(newBrokerData), newLogger())
    await broker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('request', async () => {
    const produceData = [
      {
        topic: topicName,
        partitions: [
          {
            partition: 0,
            messages: [{ key: `key-0`, value: `some-value-0` }],
          },
        ],
      },
    ]

    await broker.produce({ topicData: produceData })

    const topics = [
      {
        topic: topicName,
        partitions: [{ partition: 0 }],
      },
    ]

    const response = await broker.listOffsets({ topics })
    expect(response).toEqual({
      responses: [
        {
          topic: topicName,
          partitions: expect.arrayContaining([
            {
              errorCode: 0,
              offsets: expect.arrayContaining([expect.stringMatching(/\d+/)]),
              partition: 0,
            },
          ]),
        },
      ],
    })
  })

  test('request different points of interest', async () => {
    const produceData = () => [
      {
        topic: topicName,
        partitions: [
          {
            partition: 0,
            messages: [
              { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
              { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
            ],
          },
        ],
      },
    ]

    await broker.produce({ topicData: produceData() })
    await broker.produce({ topicData: produceData() })

    let topics = [
      {
        topic: topicName,
        partitions: [{ partition: 0, timestamp: LATEST }],
      },
    ]

    let response = await broker.listOffsets({ topics })
    expect(response).toEqual({
      responses: [
        {
          topic: topicName,
          partitions: expect.arrayContaining([
            {
              errorCode: 0,
              offsets: expect.arrayContaining(['4']),
              partition: 0,
            },
          ]),
        },
      ],
    })

    topics = [
      {
        topic: topicName,
        partitions: [{ partition: 0, timestamp: EARLIEST }],
      },
    ]

    response = await broker.listOffsets({ topics })
    expect(response).toEqual({
      responses: [
        {
          topic: topicName,
          partitions: expect.arrayContaining([
            {
              errorCode: 0,
              offsets: expect.arrayContaining(['0']),
              partition: 0,
            },
          ]),
        },
      ],
    })
  })
})
