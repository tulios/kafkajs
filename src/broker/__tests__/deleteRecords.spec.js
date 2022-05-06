const Broker = require('../index')
const createProducer = require('../../producer')

const {
  createCluster,
  newLogger,
  createTopic,
  secureRandom,
  createModPartitioner,
  createConnectionPool,
  retryProtocol,
} = require('testHelpers')

const { KafkaJSProtocolError } = require('../../errors.js')

describe('Broker > deleteRecords', () => {
  let topicName, cluster, seedBroker, producer, broker, metadata, partitionLeader, recordsToDelete

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    cluster = createCluster()

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    await createTopic({ topic: topicName })

    await Promise.all([seedBroker.connect(), producer.connect()])

    metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await seedBroker.metadata([topicName])
    )
    partitionLeader = metadata.topicMetadata[0].partitionMetadata[0].leader

    const messages = Array(10)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })

    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }],
          fromBeginning: false,
        },
      ])
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          partitions: expect.arrayContaining([expect.objectContaining({ offset: '10' })]),
        }),
      ])
    )

    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }],
          fromBeginning: true,
        },
      ])
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          partitions: expect.arrayContaining([expect.objectContaining({ offset: '0' })]),
        }),
      ])
    )

    recordsToDelete = [
      {
        topic: topicName,
        partitions: [
          {
            partition: 0,
            offset: '7',
          },
        ],
      },
    ]
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    const brokerData = metadata.brokers.find(b => b.nodeId === partitionLeader)

    broker = new Broker({
      connectionPool: createConnectionPool(brokerData),
      logger: newLogger(),
    })
    await broker.connect()

    const response = await broker.deleteRecords({ topics: recordsToDelete })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topics: [
        {
          topic: topicName,
          partitions: [
            {
              partition: 0,
              lowWatermark: 7n,
              errorCode: 0,
            },
          ],
        },
      ],
    })
  })

  test('rejects the promise when offset is incorrect', async () => {
    recordsToDelete[0].partitions[0].offset = '11'
    const brokerData = metadata.brokers.find(b => b.nodeId === partitionLeader)
    broker = new Broker({
      connectionPool: createConnectionPool(brokerData),
      logger: newLogger(),
    })
    await broker.connect()

    let error
    try {
      await broker.deleteRecords({ topics: recordsToDelete })
    } catch (e) {
      error = e
    }

    expect(error).toBeDefined()
    expect(error.name).toBe('KafkaJSDeleteTopicRecordsError')
    expect(error.partitions[0].error).toStrictEqual(
      new KafkaJSProtocolError(
        'The requested offset is not within the range of offsets maintained by the server'
      )
    )
  })

  test('rejects the promise when broker is not the partition leader', async () => {
    const brokerData = metadata.brokers.find(b => b.nodeId !== partitionLeader)
    broker = new Broker({
      connectionPool: createConnectionPool(brokerData),
      logger: newLogger(),
    })
    await broker.connect()

    let error
    try {
      await broker.deleteRecords({ topics: recordsToDelete })
    } catch (e) {
      error = e
    }

    expect(error).toBeDefined()
    expect(error.name).toBe('KafkaJSDeleteTopicRecordsError')
    expect(error.partitions[0].error).toStrictEqual(
      new KafkaJSProtocolError('This server is not the leader for that topic-partition')
    )
  })

  describe('when the broker has multiple partitions', () => {
    let secondTopicName, testPartitions

    beforeEach(async () => {
      partitionLeader = 0
      secondTopicName = `test-topic-${secureRandom()}`

      // 3 partitions per broker
      await createTopic({ topic: secondTopicName, partitions: 9 })

      metadata = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await seedBroker.metadata([secondTopicName])
      )
      testPartitions = metadata.topicMetadata[0].partitionMetadata
        .filter(({ leader }) => leader === partitionLeader)
        .map(({ partitionId }) => partitionId)

      const messages = Array(30)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })
      producer = createProducer({
        cluster,
        // send all messages to 1 partition, which will be the only successful one
        createPartitioner: () => () => testPartitions[0],
        logger: newLogger(),
      })
      await producer.connect()
      await producer.send({ acks: 1, topic: secondTopicName, messages })

      recordsToDelete = [
        {
          topic: secondTopicName,
          partitions: [
            {
              partition: testPartitions[0],
              offset: '7',
            },
            {
              partition: testPartitions[1],
              offset: '98',
            },
            {
              partition: testPartitions[2],
              offset: '99',
            },
          ],
        },
      ]
    })

    test('rejects the promise with all partition errors', async () => {
      const brokerData = metadata.brokers.find(b => b.nodeId === partitionLeader)
      broker = new Broker({
        connectionPool: createConnectionPool(brokerData),
        logger: newLogger(),
      })
      await broker.connect()

      let error
      try {
        await broker.deleteRecords({ topics: recordsToDelete })
      } catch (e) {
        error = e
      }

      expect(error).toBeDefined()
      expect(error.name).toEqual('KafkaJSDeleteTopicRecordsError')
      expect(error.partitions).toEqual(
        expect.arrayContaining([
          {
            partition: testPartitions[1],
            offset: '98',
            error: new KafkaJSProtocolError(
              'The requested offset is not within the range of offsets maintained by the server'
            ),
          },
          {
            partition: testPartitions[2],
            offset: '99',
            error: new KafkaJSProtocolError(
              'The requested offset is not within the range of offsets maintained by the server'
            ),
          },
        ])
      )
    })
  })
})
