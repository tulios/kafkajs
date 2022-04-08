const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { Types: Compression } = require('../../protocol/message/compression')
const { KafkaJSProtocolError } = require('../../errors')
const {
  secureRandom,
  createConnectionPool,
  newLogger,
  createTopic,
  testIfKafkaAtLeast_0_11,
  retryProtocol,
} = require('testHelpers')

describe('Broker > Produce', () => {
  let topicName, broker, broker2
  const timestamp = 1509928155660

  const createHeader = () => ({
    headers: { [`hkey-${secureRandom()}`]: `hvalue-${secureRandom()}` },
  })

  const createTopicData = ({ headers = false, firstSequence = 0 } = {}) => [
    {
      topic: topicName,
      partitions: [
        {
          partition: 0,
          firstSequence,
          messages: [
            {
              key: `key-${secureRandom()}`,
              value: `some-value-${secureRandom()}`,
              timestamp,
              ...(headers ? createHeader() : {}),
            },
            {
              key: `key-${secureRandom()}`,
              value: `some-value-${secureRandom()}`,
              timestamp,
              ...(headers ? createHeader() : {}),
            },
            {
              key: `key-${secureRandom()}`,
              value: `some-value-${secureRandom()}`,
              timestamp,
              ...(headers ? createHeader() : {}),
            },
          ],
        },
      ],
    },
  ]

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    broker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await broker.connect()
    await createTopic({ topic: topicName })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
    broker2 && (await broker2.disconnect())
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await broker.disconnect()
    broker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await expect(broker.produce({ topicData: [] })).rejects.toEqual(
      new Error('Broker not connected')
    )
  })

  test('request', async () => {
    const metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker.metadata([topicName])
    )

    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker2 = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
    await broker2.connect()

    const response1 = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker2.produce({ topicData: createTopicData() })
    )
    expect(response1).toEqual({
      topics: [
        {
          topicName,
          partitions: [
            {
              errorCode: 0,
              baseOffset: '0',
              partition: 0,
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })

    const response2 = await broker2.produce({ topicData: createTopicData() })
    expect(response2).toEqual({
      topics: [
        {
          topicName,
          partitions: [
            {
              errorCode: 0,
              baseOffset: '3',
              partition: 0,
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })
  })

  test('request with GZIP', async () => {
    const metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await broker.metadata([topicName])
    )

    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker2 = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
    await broker2.connect()

    const response1 = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () =>
        await broker2.produce({
          compression: Compression.GZIP,
          topicData: createTopicData(),
        })
    )

    expect(response1).toEqual({
      topics: [
        {
          topicName,
          partitions: [
            {
              errorCode: 0,
              baseOffset: '0',
              partition: 0,
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })

    const response2 = await broker2.produce({
      compression: Compression.GZIP,
      topicData: createTopicData(),
    })

    expect(response2).toEqual({
      topics: [
        {
          topicName,
          partitions: [
            {
              errorCode: 0,
              baseOffset: '3',
              partition: 0,
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })
  })

  describe('Record batch', () => {
    testIfKafkaAtLeast_0_11('request', async () => {
      const metadata = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker.metadata([topicName])
      )

      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })
      await broker2.connect()

      const response1 = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker2.produce({ topicData: createTopicData() })
      )

      expect(response1).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '0',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })

      const response2 = await broker2.produce({ topicData: createTopicData() })
      expect(response2).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '3',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })
    })

    testIfKafkaAtLeast_0_11('request with idempotent producer', async () => {
      // Get producer id & epoch
      const {
        coordinator: { host, port },
      } = await retryProtocol(
        'GROUP_COORDINATOR_NOT_AVAILABLE',
        async () =>
          await broker.findGroupCoordinator({
            groupId: `group-${secureRandom()}`,
            coordinatorType: COORDINATOR_TYPES.GROUP,
          })
      )

      const producerBroker = new Broker({
        connectionPool: createConnectionPool({ host, port }),
        logger: newLogger(),
      })

      await producerBroker.connect()
      const result = await producerBroker.initProducerId({
        transactionTimeout: 30000,
      })

      const producerId = result.producerId
      const producerEpoch = result.producerEpoch

      const metadata = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker.metadata([topicName])
      )

      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })
      await broker2.connect()

      const response1 = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () =>
          await broker2.produce({
            producerId,
            producerEpoch,
            topicData: createTopicData({ headers: false }),
          })
      )

      expect(response1).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '0',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })

      // We have to synchronize the sequence number between the producer and the broker
      await expect(
        broker2.produce({
          producerId,
          producerEpoch,
          topicData: createTopicData({ headers: false, firstSequence: 1 }), // Too small
        })
      ).rejects.toEqual(
        new KafkaJSProtocolError('The broker received an out of order sequence number')
      )

      await expect(
        broker2.produce({
          producerId,
          producerEpoch,
          topicData: createTopicData({ headers: false, firstSequence: 5 }), // Too big
        })
      ).rejects.toEqual(
        new KafkaJSProtocolError('The broker received an out of order sequence number')
      )

      await broker2.produce({
        producerId,
        producerEpoch,
        topicData: createTopicData({ headers: false, firstSequence: 3 }), // Just right
      })
    })

    testIfKafkaAtLeast_0_11('request with headers', async () => {
      const metadata = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker.metadata([topicName])
      )

      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })
      await broker2.connect()

      const response1 = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker2.produce({ topicData: createTopicData({ headers: true }) })
      )

      expect(response1).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '0',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })

      const response2 = await broker2.produce({ topicData: createTopicData() })
      expect(response2).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '3',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })
    })

    testIfKafkaAtLeast_0_11('request with GZIP', async () => {
      const metadata = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () => await broker.metadata([topicName])
      )

      // Find leader of partition
      const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
      const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

      // Connect to the correct broker to produce message
      broker2 = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })
      await broker2.connect()

      const response1 = await retryProtocol(
        'LEADER_NOT_AVAILABLE',
        async () =>
          await broker2.produce({
            compression: Compression.GZIP,
            topicData: createTopicData(),
          })
      )

      expect(response1).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '0',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })

      const response2 = await broker2.produce({
        compression: Compression.GZIP,
        topicData: createTopicData(),
      })

      expect(response2).toEqual({
        topics: [
          {
            topicName,
            partitions: [
              {
                baseOffset: '3',
                errorCode: 0,
                logAppendTime: '-1',
                logStartOffset: '0',
                partition: 0,
              },
            ],
          },
        ],
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
      })
    })

    testIfKafkaAtLeast_0_11(
      'request to a topic with max timestamp difference configured',
      async () => {
        topicName = `test-max-timestamp-difference-${secureRandom()}`

        await createTopic({
          topic: topicName,
          config: [
            {
              name: 'message.timestamp.difference.max.ms',
              value: '604800000', // 7 days
            },
          ],
        })

        const metadata = await retryProtocol(
          'LEADER_NOT_AVAILABLE',
          async () => await broker.metadata([topicName])
        )

        // Find leader of partition
        const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
        const newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

        // Connect to the correct broker to produce message
        broker2 = new Broker({
          connectionPool: createConnectionPool(newBrokerData),
          logger: newLogger(),
        })
        await broker2.connect()

        const partitionData = {
          topic: topicName,
          partitions: [
            {
              partition: 0,
              messages: [{ key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` }],
            },
          ],
        }

        const response1 = await retryProtocol(
          'LEADER_NOT_AVAILABLE',
          async () => await broker2.produce({ topicData: [partitionData] })
        )

        expect(response1).toEqual({
          topics: [
            {
              topicName,
              partitions: [
                {
                  baseOffset: '0',
                  errorCode: 0,
                  logAppendTime: '-1',
                  logStartOffset: '0',
                  partition: 0,
                },
              ],
            },
          ],
          clientSideThrottleTime: expect.optional(0),
          throttleTime: 0,
        })
      }
    )
  })
})
