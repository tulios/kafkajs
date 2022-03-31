const Broker = require('../index')
const createProducer = require('../../producer')
const createRetrier = require('../../retry')

const {
  secureRandom,
  createConnectionPool,
  createCluster,
  newLogger,
  createTopic,
  retryProtocol,
  testIfKafkaAtMost_0_10,
  testIfKafkaAtLeast_0_11,
  generateMessages,
} = require('testHelpers')
const { Types: Compression } = require('../../protocol/message/compression')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')

const minBytes = 1
const maxBytes = 10485760 // 10MB
const maxBytesPerPartition = 1048576 // 1MB
const maxWaitTime = 100
const timestamp = 1509827900073

describe('Broker > Fetch', () => {
  let topicName, seedBroker, broker, newBrokerData

  const headerFor = message => {
    const keys = Object.keys(message.headers)
    return { [keys[0]]: Buffer.from(message.headers[keys[0]]) }
  }

  const createMessages = (n = 0, headers = false) => [
    {
      key: `key-${n}`,
      value: `some-value-${n}`,
      timestamp,
      ...(!headers ? {} : { headers: { [`header-key-${n}`]: `header-value-${n}` } }),
    },
    {
      key: `key-${n + 1}`,
      value: `some-value-${n + 1}`,
      timestamp,
      ...(!headers ? {} : { headers: { [`header-key-${n + 1}`]: `header-value-${n + 1}` } }),
    },
    {
      key: `key-${n + 2}`,
      value: `some-value-${n + 2}`,
      timestamp,
      ...(!headers ? {} : { headers: { [`header-key-${n + 2}`]: `header-value-${n + 2}` } }),
    },
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

  const expectedBatchContext = (options = {}) => ({
    firstOffset: expect.any(String),
    firstSequence: expect.any(Number),
    firstTimestamp: expect.any(String),
    inTransaction: false,
    isControlBatch: false,
    lastOffsetDelta: expect.any(Number),
    magicByte: 2,
    maxTimestamp: expect.any(String),
    partitionLeaderEpoch: expect.any(Number),
    producerEpoch: 0,
    producerId: '-1',
    timestampType: 0,
    ...options,
  })

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await seedBroker.connect()
    await createTopic({ topic: topicName })

    const metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await seedBroker.metadata([topicName])
    )

    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    newBrokerData = metadata.brokers.find(b => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
    await broker.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('rejects the Promise if lookupRequest is not defined', async () => {
    await broker.disconnect()
    broker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
    await expect(broker.fetch({ topics: [] })).rejects.toEqual(new Error('Broker not connected'))
  })

  testIfKafkaAtMost_0_10('request', async () => {
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
      clientSideThrottleTime: expect.optional(0),
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

    topicData = createTopicData(targetPartition, createMessages(1))
    await broker.produce({ topicData })
    fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
  })

  testIfKafkaAtMost_0_10('request with GZIP', async () => {
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
      clientSideThrottleTime: expect.optional(0),
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

    topicData = createTopicData(targetPartition, createMessages(1))
    await broker.produce({ topicData, compression: Compression.GZIP })
    fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
    expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
  })

  describe('Record batch', () => {
    beforeEach(async () => {
      await broker.disconnect()

      broker = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })
      await broker.connect()
    })

    testIfKafkaAtLeast_0_11('request', async () => {
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
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
        errorCode: 0,
        sessionId: 0,
        responses: [
          {
            topicName,
            partitions: [
              {
                abortedTransactions: [],
                errorCode: 0,
                highWatermark: '3',
                lastStableOffset: '3',
                lastStartOffset: '0',
                partition: 0,
                preferredReadReplica: expect.optional(-1),
                messages: [
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '0',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[0].key),
                    value: Buffer.from(messages[0].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '1',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[1].key),
                    value: Buffer.from(messages[1].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '2',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[2].key),
                    value: Buffer.from(messages[2].value),
                    isControlRecord: false,
                  },
                ],
              },
            ],
          },
        ],
      })

      topicData = createTopicData(targetPartition, createMessages(1))
      await broker.produce({ topicData })
      fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
      expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
    })

    testIfKafkaAtLeast_0_11('request with headers', async () => {
      const targetPartition = 0
      const messages = createMessages(0, true)
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
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
        errorCode: 0,
        sessionId: 0,
        responses: [
          {
            topicName,
            partitions: [
              {
                abortedTransactions: [],
                errorCode: 0,
                highWatermark: '3',
                lastStableOffset: '3',
                lastStartOffset: '0',
                partition: 0,
                preferredReadReplica: expect.optional(-1),
                messages: [
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '0',
                    timestamp: '1509827900073',
                    headers: headerFor(messages[0]),
                    key: Buffer.from(messages[0].key),
                    value: Buffer.from(messages[0].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '1',
                    timestamp: '1509827900073',
                    headers: headerFor(messages[1]),
                    key: Buffer.from(messages[1].key),
                    value: Buffer.from(messages[1].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '2',
                    timestamp: '1509827900073',
                    headers: headerFor(messages[2]),
                    key: Buffer.from(messages[2].key),
                    value: Buffer.from(messages[2].value),
                    isControlRecord: false,
                  },
                ],
              },
            ],
          },
        ],
      })

      topicData = createTopicData(targetPartition, createMessages(1, true))
      await broker.produce({ topicData })
      fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
      expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
    })

    testIfKafkaAtLeast_0_11('request with GZIP', async () => {
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
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
        errorCode: 0,
        sessionId: 0,
        responses: [
          {
            topicName,
            partitions: [
              {
                abortedTransactions: [],
                errorCode: 0,
                highWatermark: '3',
                lastStableOffset: '3',
                lastStartOffset: '0',
                partition: 0,
                preferredReadReplica: expect.optional(-1),
                messages: [
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '0',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[0].key),
                    value: Buffer.from(messages[0].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '1',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[1].key),
                    value: Buffer.from(messages[1].value),
                    isControlRecord: false,
                  },
                  {
                    magicByte: 2,
                    attributes: 0,
                    batchContext: expectedBatchContext(),
                    offset: '2',
                    timestamp: '1509827900073',
                    headers: {},
                    key: Buffer.from(messages[2].key),
                    value: Buffer.from(messages[2].value),
                    isControlRecord: false,
                  },
                ],
              },
            ],
          },
        ],
      })

      topicData = createTopicData(targetPartition, createMessages(1))
      await broker.produce({ topicData, compression: Compression.GZIP })
      fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
      expect(fetchResponse.responses[0].partitions[0].highWatermark).toEqual('6')
    })
  })

  describe('transactional', () => {
    let transactionalId, producer, retry

    beforeEach(async () => {
      transactionalId = `transactional-id-${secureRandom()}`

      producer = createProducer({
        cluster: createCluster(),
        logger: newLogger(),
        transactionalId,
        maxInFlightRequests: 1,
      })

      broker = new Broker({
        connectionPool: createConnectionPool(newBrokerData),
        logger: newLogger(),
      })

      await broker.connect()
      await producer.connect()

      retry = createRetrier({ retries: 5 })
    })

    afterEach(async () => {
      await producer.disconnect()
      await broker.disconnect()
    })

    testIfKafkaAtLeast_0_11(
      'returns transactional messages only after transaction has ended for an isolation level of "read_committed" (default)',
      async () => {
        const targetPartition = 0
        const messages = generateMessages({ prefix: 'aborted', number: 3 }).map(m => ({
          ...m,
          partition: targetPartition,
        }))

        const txn = await producer.transaction()
        await txn.send({
          topic: topicName,
          messages,
        })

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
          clientSideThrottleTime: expect.optional(0),
          throttleTime: 0,
          errorCode: 0,
          sessionId: 0,
          responses: [
            {
              topicName,
              partitions: [
                {
                  abortedTransactions: [],
                  lastStableOffset: '0',
                  lastStartOffset: '0',
                  errorCode: 0,
                  highWatermark: '3',
                  partition: 0,
                  preferredReadReplica: expect.optional(-1),
                  messages: [],
                },
              ],
            },
          ],
        })

        await txn.abort()

        // It appears there can be a delay between the EndTxn response
        // and the control record appearing in the partition, likely because the
        // transaction coordinator does not wait for the control record to
        // be fully replicated. Retry since it should eventually appear.
        // @see https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#bookmark=id.3af5934pfogc
        await retry(async () => {
          fetchResponse = await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })
          expect(fetchResponse).toEqual({
            clientSideThrottleTime: expect.optional(0),
            throttleTime: 0,
            errorCode: 0,
            sessionId: 0,
            responses: [
              {
                topicName,
                partitions: [
                  {
                    abortedTransactions: [
                      {
                        firstOffset: '0',
                        producerId: expect.any(String),
                      },
                    ],
                    errorCode: 0,
                    highWatermark: '4', // Number of produced messages + 1 control record
                    lastStableOffset: '4',
                    lastStartOffset: '0',
                    partition: 0,
                    preferredReadReplica: expect.optional(-1),
                    messages: [
                      {
                        magicByte: 2,
                        attributes: 0,
                        batchContext: expectedBatchContext({
                          producerEpoch: expect.any(Number),
                          producerId: expect.any(String),
                          inTransaction: true,
                        }),
                        offset: '0',
                        timestamp: expect.any(String),
                        headers: {},
                        key: Buffer.from(messages[0].key),
                        value: Buffer.from(messages[0].value),
                        isControlRecord: false,
                      },
                      {
                        magicByte: 2,
                        attributes: 0,
                        batchContext: expectedBatchContext({
                          producerEpoch: expect.any(Number),
                          producerId: expect.any(String),
                          inTransaction: true,
                        }),
                        offset: '1',
                        timestamp: expect.any(String),
                        headers: {},
                        key: Buffer.from(messages[1].key),
                        value: Buffer.from(messages[1].value),
                        isControlRecord: false,
                      },
                      {
                        magicByte: 2,
                        attributes: 0,
                        batchContext: expectedBatchContext({
                          producerEpoch: expect.any(Number),
                          producerId: expect.any(String),
                          inTransaction: true,
                        }),
                        offset: '2',
                        timestamp: expect.any(String),
                        headers: {},
                        key: Buffer.from(messages[2].key),
                        value: Buffer.from(messages[2].value),
                        isControlRecord: false,
                      },
                      // Control record
                      {
                        magicByte: 2,
                        attributes: 0,
                        batchContext: expectedBatchContext({
                          producerEpoch: expect.any(Number),
                          producerId: expect.any(String),
                          inTransaction: true,
                          isControlBatch: true,
                        }),
                        offset: '3',
                        timestamp: expect.any(String),
                        key: Buffer.from([0, 0, 0, 0]), // Aborted
                        value: expect.any(Buffer),
                        headers: {},
                        isControlRecord: true,
                      },
                    ],
                  },
                ],
              },
            ],
          })
        })
      }
    )

    testIfKafkaAtLeast_0_11(
      'returns transactional messages immediately for an isolation level of "read_uncommitted"',
      async () => {
        const targetPartition = 0
        const messages = generateMessages({ prefix: 'aborted', number: 3 }).map(m => ({
          ...m,
          partition: targetPartition,
        }))

        const txn = await producer.transaction()
        await txn.send({
          topic: topicName,
          messages,
        })

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

        let fetchResponse = await broker.fetch({
          maxWaitTime,
          minBytes,
          maxBytes,
          topics,
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
        expect(fetchResponse).toEqual({
          clientSideThrottleTime: expect.optional(0),
          throttleTime: 0,
          errorCode: 0,
          sessionId: 0,
          responses: [
            {
              topicName,
              partitions: [
                {
                  abortedTransactions: [], // None of these messages have been aborted yet
                  errorCode: 0,
                  highWatermark: '3', // Number of produced messages
                  // The end offset of a partition for a read_committed consumer would be the
                  // offset of the first message in the partition belonging to an open transaction.
                  //
                  // Note: In version < 2 this was '-1'
                  lastStableOffset: '0',
                  lastStartOffset: '0',
                  partition: 0,
                  preferredReadReplica: expect.optional(-1),
                  messages: [
                    {
                      magicByte: 2,
                      attributes: 0,
                      batchContext: expectedBatchContext({
                        producerEpoch: expect.any(Number),
                        producerId: expect.any(String),
                        inTransaction: true,
                      }),
                      offset: '0',
                      timestamp: expect.any(String),
                      headers: {},
                      key: Buffer.from(messages[0].key),
                      value: Buffer.from(messages[0].value),
                      isControlRecord: false,
                    },
                    {
                      magicByte: 2,
                      attributes: 0,
                      batchContext: expectedBatchContext({
                        producerEpoch: expect.any(Number),
                        producerId: expect.any(String),
                        inTransaction: true,
                      }),
                      offset: '1',
                      timestamp: expect.any(String),
                      headers: {},
                      key: Buffer.from(messages[1].key),
                      value: Buffer.from(messages[1].value),
                      isControlRecord: false,
                    },
                    {
                      magicByte: 2,
                      attributes: 0,
                      batchContext: expectedBatchContext({
                        producerEpoch: expect.any(Number),
                        producerId: expect.any(String),
                        inTransaction: true,
                      }),
                      offset: '2',
                      timestamp: expect.any(String),
                      headers: {},
                      key: Buffer.from(messages[2].key),
                      value: Buffer.from(messages[2].value),
                      isControlRecord: false,
                    },
                  ],
                },
              ],
            },
          ],
        })

        await txn.abort()

        // Although the messages are aborted, they will still be there in the log,
        // so the offset will still increase
        topics[0].partitions[0].fetchOffset = 3

        // It appears there can be a delay between the EndTxn response
        // and the control record appearing in the partition, likely because the
        // transaction coordinator does not wait for the control record to
        // be fully replicated. Retry since it should eventually appear.
        // @see https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#bookmark=id.3af5934pfogc
        await retry(async () => {
          fetchResponse = await broker.fetch({
            maxWaitTime,
            minBytes,
            maxBytes,
            topics,
            isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
          })
          expect(fetchResponse).toEqual({
            clientSideThrottleTime: expect.optional(0),
            throttleTime: 0,
            errorCode: 0,
            sessionId: 0,
            responses: [
              {
                topicName,
                partitions: [
                  {
                    abortedTransactions: [],
                    errorCode: 0,
                    highWatermark: '4',
                    lastStableOffset: '4',
                    lastStartOffset: '0',
                    partition: 0,
                    preferredReadReplica: expect.optional(-1),
                    messages: [
                      // Control record
                      {
                        magicByte: 2,
                        attributes: 0,
                        batchContext: expectedBatchContext({
                          producerEpoch: expect.any(Number),
                          producerId: expect.any(String),
                          inTransaction: true,
                          isControlBatch: true,
                        }),
                        offset: '3',
                        timestamp: expect.any(String),
                        key: Buffer.from([0, 0, 0, 0]), // Aborted
                        value: expect.any(Buffer),
                        headers: {},
                        isControlRecord: true,
                      },
                    ],
                  },
                ],
              },
            ],
          })
        })
      }
    )
  })
})
