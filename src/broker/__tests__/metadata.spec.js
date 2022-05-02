const Broker = require('../index')
const {
  secureRandom,
  createConnectionPool,
  newLogger,
  createTopic,
  retryProtocol,
  testIfKafkaAtLeast_0_11,
} = require('testHelpers')

describe('Broker > Metadata', () => {
  let topicName, broker

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
    broker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await expect(broker.metadata()).rejects.toEqual(new Error('Broker not connected'))
  })

  test('request', async () => {
    await broker.connect()
    await createTopic({ topic: topicName })

    const response = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker.metadata([topicName])
    )

    // We can run this test on both clusters with a broker.rack configuration and brokers
    // without, but that is painful to describe in jest. Work out the values for the rack
    // setting separately.
    const rackValues = response.brokers.some(({ rack }) => Boolean(rack))
    expect(response).toMatchObject({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      brokers: expect.arrayContaining([
        {
          host: 'localhost',
          nodeId: expect.any(Number),
          port: expect.any(Number),
          rack: rackValues ? expect.any(String) : null,
        },
      ]),
      clusterId: expect.stringMatching(/[a-zA-Z0-9-]/),
      controllerId: expect.any(Number),
      topicMetadata: [
        {
          isInternal: false,
          partitionMetadata: [
            {
              isr: expect.any(Array),
              leader: expect.any(Number),
              partitionErrorCode: 0,
              partitionId: 0,
              replicas: expect.any(Array),
            },
          ],
          topic: topicName,
          topicErrorCode: 0,
        },
      ],
    })
  })

  test('can fetch metadata for all topics', async () => {
    await broker.connect()
    await createTopic({ topic: topicName })
    await createTopic({ topic: `test-topic-${secureRandom()}` })

    let response = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker.metadata([])
    )

    expect(response.topicMetadata.length).toBeGreaterThanOrEqual(2)

    response = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker.metadata([topicName])
    )

    expect(response.topicMetadata.length).toEqual(1)
  })

  describe('when allowAutoTopicCreation is disabled and the topic does not exist', () => {
    beforeEach(() => {
      topicName = `test-topic-${secureRandom()}`
      broker = new Broker({
        connectionPool: createConnectionPool(),
        allowAutoTopicCreation: false,
        logger: newLogger(),
      })
    })

    testIfKafkaAtLeast_0_11('returns UNKNOWN_TOPIC_OR_PARTITION', async () => {
      await broker.connect()

      await expect(broker.metadata([topicName])).rejects.toHaveProperty(
        'type',
        'UNKNOWN_TOPIC_OR_PARTITION'
      )
    })
  })
})
