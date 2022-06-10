let initProducerIdSpy
let sendOffsetsSpy
let retrySpy

jest.mock('./eosManager', () => {
  return (...args) => {
    const eosManager = jest.requireActual('./eosManager')(...args)

    initProducerIdSpy = jest.spyOn(eosManager, 'initProducerId')
    sendOffsetsSpy = jest.spyOn(eosManager, 'sendOffsets')

    return eosManager
  }
})

jest.mock('../retry', () => {
  const spy = jest.fn().mockImplementation(jest.requireActual('../retry'))
  retrySpy = spy
  return spy
})

const uuid = require('uuid/v4')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const createProducer = require('./index')
const createConsumer = require('../consumer')
const {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslEntries,
  createCluster,
  createModPartitioner,
  sslBrokers,
  saslBrokers,
  newLogger,
  testIfKafkaAtLeast_0_11,
  createTopic,
  waitForMessages,
} = require('testHelpers')
const createRetrier = require('../retry')
const { KafkaJSNonRetriableError } = require('../errors')
const sleep = require('../utils/sleep')

describe('Producer', () => {
  let topicName, producer, consumer

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    consumer && (await consumer.disconnect())
  })

  test('throws an error if the topic is invalid', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await expect(producer.send({ acks: 1, topic: null })).rejects.toHaveProperty(
      'message',
      'Invalid topic'
    )
  })

  test('throws an error if messages is invalid', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await expect(
      producer.send({ acks: 1, topic: topicName, messages: null })
    ).rejects.toHaveProperty('message', `Invalid messages array [null] for topic "${topicName}"`)
  })

  test('throws an error for messages with a value of undefined', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })

    await expect(
      producer.send({ acks: 1, topic: topicName, messages: [{ foo: 'bar' }] })
    ).rejects.toHaveProperty(
      'message',
      `Invalid message without value for topic "${topicName}": {"foo":"bar"}`
    )
  })

  test('throws an error if the producer is not connected', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await expect(
      producer.send({
        topic: topicName,
        messages: [{ key: 'key', value: 'value' }],
      })
    ).rejects.toThrow(/The producer is disconnected/)
  })

  test('throws an error if the producer is disconnecting', async () => {
    const cluster = createCluster()
    const originalDisconnect = cluster.disconnect
    cluster.disconnect = async () => {
      await sleep(10)
      return originalDisconnect.apply(cluster)
    }

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    producer.disconnect() // slow disconnect should give a disconnecting status
    await expect(
      producer.send({
        topic: topicName,
        messages: [{ key: 'key', value: 'value' }],
      })
    ).rejects.toThrow(/The producer is disconnecting/)
    cluster.disconnect = originalDisconnect
  })

  test('allows messages with a null value to support tombstones', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await producer.connect()
    await producer.send({ acks: 1, topic: topicName, messages: [{ foo: 'bar', value: null }] })
  })

  test('support SSL connections', async () => {
    const cluster = createCluster(sslConnectionOpts(), sslBrokers())
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  for (const e of saslEntries) {
    test(`support SASL ${e.name} connections`, async () => {
      const cluster = createCluster(e.opts(), saslBrokers())
      producer = createProducer({ cluster, logger: newLogger() })
      await producer.connect()
    })

    if (e.wrongOpts) {
      test(`throws an error if SASL ${e.name} fails to authenticate`, async () => {
        const cluster = createCluster(e.wrongOpts(), saslBrokers())

        producer = createProducer({ cluster, logger: newLogger() })
        await expect(producer.connect()).rejects.toThrow(e.expectedErr)
      })
    }
  }

  test('reconnects the cluster if disconnected', async () => {
    const cluster = createCluster({
      createPartitioner: createModPartitioner,
    })

    await createTopic({ topic: topicName })

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
    await producer.send({
      acks: 1,
      topic: topicName,
      messages: [{ key: '1', value: '1' }],
    })

    expect(cluster.isConnected()).toEqual(true)
    await cluster.disconnect()
    expect(cluster.isConnected()).toEqual(false)

    await producer.send({
      acks: 1,
      topic: topicName,
      messages: [{ key: '2', value: '2' }],
    })

    expect(cluster.isConnected()).toEqual(true)
  })

  test('gives access to its logger', () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    expect(producer.logger()).toMatchSnapshot()
  })

  test('on throws an error when provided with an invalid event name', () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })

    expect(() => producer.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      /Event name should be one of producer.events./
    )
  })

  test('emits connection events', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    const connectListener = jest.fn().mockName('connect')
    const disconnectListener = jest.fn().mockName('disconnect')
    producer.on(producer.events.CONNECT, connectListener)
    producer.on(producer.events.DISCONNECT, disconnectListener)

    await producer.connect()
    expect(connectListener).toHaveBeenCalled()

    await producer.disconnect()
    expect(disconnectListener).toHaveBeenCalled()
  })

  test('emits the request event', async () => {
    const emitter = new InstrumentationEventEmitter()
    producer = createProducer({
      logger: newLogger(),
      cluster: createCluster({ instrumentationEmitter: emitter }),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request')
    producer.on(producer.events.REQUEST, requestListener)

    await producer.connect()
    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'producer.network.request',
      payload: {
        apiKey: expect.any(Number),
        apiName: 'ApiVersions',
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        duration: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
        size: expect.any(Number),
      },
    })
  })

  test('emits the request timeout event', async () => {
    const emitter = new InstrumentationEventEmitter()
    const cluster = createCluster({
      requestTimeout: 1,
      enforceRequestTimeout: true,
      instrumentationEmitter: emitter,
    })

    producer = createProducer({
      cluster,
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request_timeout')
    producer.on(producer.events.REQUEST_TIMEOUT, requestListener)

    await producer
      .connect()
      .then(() =>
        producer.send({
          acks: -1,
          topic: topicName,
          messages: [{ key: 'key-0', value: 'value-0' }],
        })
      )
      .catch(e => e)

    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'producer.network.request_timeout',
      payload: {
        apiKey: expect.any(Number),
        apiName: expect.any(String),
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
      },
    })
  })

  test('emits the request queue size event', async () => {
    await createTopic({ topic: topicName, partitions: 8 })

    const emitter = new InstrumentationEventEmitter()
    const cluster = createCluster({
      instrumentationEmitter: emitter,
      maxInFlightRequests: 1,
      clientId: 'test-client-id11111',
    })

    producer = createProducer({
      cluster,
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request_queue_size')
    producer.on(producer.events.REQUEST_QUEUE_SIZE, requestListener)

    await producer.connect()
    await Promise.all([
      producer.send({
        acks: -1,
        topic: topicName,
        messages: [
          { partition: 0, value: 'value-0' },
          { partition: 1, value: 'value-1' },
          { partition: 2, value: 'value-2' },
        ],
      }),
      producer.send({
        acks: -1,
        topic: topicName,
        messages: [
          { partition: 0, value: 'value-0' },
          { partition: 1, value: 'value-1' },
          { partition: 2, value: 'value-2' },
        ],
      }),
    ])

    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'producer.network.request_queue_size',
      payload: {
        broker: expect.any(String),
        clientId: expect.any(String),
        queueSize: expect.any(Number),
      },
    })
  })

  describe('when acks=0', () => {
    it('returns immediately', async () => {
      const cluster = createCluster({
        ...connectionOpts(),
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger() })
      await producer.connect()

      const sendMessages = async () =>
        await producer.send({
          acks: 0,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        })

      expect(await sendMessages()).toEqual([])
    })
  })

  function testProduceMessages(idempotent = false) {
    const acks = idempotent ? -1 : 1

    test('produce messages', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      const sendMessages = async () =>
        await producer.send({
          acks,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        })

      expect(await sendMessages()).toEqual([
        {
          baseOffset: '0',
          topicName,
          errorCode: 0,
          partition: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          baseOffset: '10',
          topicName,
          errorCode: 0,
          partition: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
        },
      ])
    })

    test('it should allow sending an empty list of messages', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      await expect(producer.send({ acks, topic: topicName, messages: [] })).toResolve()
    })

    test('produce messages to multiple topics', async () => {
      const topics = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]

      await createTopic({ topic: topics[0] })
      await createTopic({ topic: topics[1] })

      const cluster = createCluster({
        ...connectionOpts(),
        createPartitioner: createModPartitioner,
      })
      const byTopicName = (a, b) => a.topicName.localeCompare(b.topicName)

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      const sendBatch = async topics => {
        const topicMessages = topics.map(topic => ({
          acks,
          topic,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        }))

        return producer.sendBatch({
          acks,
          topicMessages,
        })
      }

      let result = await sendBatch(topics)
      expect(result.sort(byTopicName)).toEqual(
        [
          {
            baseOffset: '0',
            topicName: topics[0],
            errorCode: 0,
            partition: 0,
            logStartOffset: '0',
            logAppendTime: '-1',
          },
          {
            topicName: topics[1],
            errorCode: 0,
            baseOffset: '0',
            partition: 0,
            logStartOffset: '0',
            logAppendTime: '-1',
          },
        ].sort(byTopicName)
      )

      result = await sendBatch(topics)
      expect(result.sort(byTopicName)).toEqual(
        [
          {
            topicName: topics[0],
            errorCode: 0,
            baseOffset: '10',
            partition: 0,
            logAppendTime: '-1',
            logStartOffset: '0',
          },
          {
            topicName: topics[1],
            errorCode: 0,
            baseOffset: '10',
            partition: 0,
            logAppendTime: '-1',
            logStartOffset: '0',
          },
        ].sort(byTopicName)
      )
    })

    test('sendBatch should allow sending an empty list of topicMessages', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      await expect(producer.sendBatch({ acks, topicMessages: [] })).toResolve()
    })

    test('sendBatch should consolidate topicMessages by topic', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName, partitions: 1 })

      const messagesConsumed = []
      consumer = createConsumer({
        groupId: `test-consumer-${uuid()}`,
        cluster: createCluster(),
        logger: newLogger(),
      })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      await consumer.run({
        eachMessage: async event => {
          messagesConsumed.push(event)
        },
      })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      const topicMessages = [
        {
          topic: topicName,
          messages: [
            { key: 'key-1', value: 'value-1' },
            { key: 'key-2', value: 'value-2' },
          ],
        },
        {
          topic: topicName,
          messages: [{ key: 'key-3', value: 'value-3' }],
        },
      ]

      const result = await producer.sendBatch({
        acks,
        topicMessages,
      })
      expect(result).toEqual([
        {
          topicName,
          errorCode: 0,
          baseOffset: '0',
          partition: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
        },
      ])

      await waitForMessages(messagesConsumed, { number: 3 })
      await expect(waitForMessages(messagesConsumed, { number: 3 })).resolves.toEqual([
        expect.objectContaining({
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({
            key: Buffer.from('key-1'),
            value: Buffer.from('value-1'),
            offset: '0',
          }),
        }),
        expect.objectContaining({
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({
            key: Buffer.from('key-2'),
            value: Buffer.from('value-2'),
            offset: '1',
          }),
        }),
        expect.objectContaining({
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({
            key: Buffer.from('key-3'),
            value: Buffer.from('value-3'),
            offset: '2',
          }),
        }),
      ])
    })

    testIfKafkaAtLeast_0_11('produce messages for Kafka 0.11', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      const sendMessages = async () =>
        await producer.send({
          acks,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        })

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '0',
          errorCode: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
          partition: 0,
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '10',
          errorCode: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
          partition: 0,
        },
      ])
    })

    testIfKafkaAtLeast_0_11(
      'produce messages for Kafka 0.11 without specifying message key',
      async () => {
        const cluster = createCluster({
          createPartitioner: createModPartitioner,
        })

        await createTopic({ topic: topicName })

        producer = createProducer({ cluster, logger: newLogger(), idempotent })
        await producer.connect()

        await expect(
          producer.send({
            acks,
            topic: topicName,
            messages: [
              {
                value: 'test-value',
              },
            ],
          })
        ).toResolve()
      }
    )

    testIfKafkaAtLeast_0_11('produce messages for Kafka 0.11 with headers', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({ cluster, logger: newLogger(), idempotent })
      await producer.connect()

      const sendMessages = async () =>
        await producer.send({
          acks,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
            headers: {
              [`header-a${i}`]: `header-value-a${i}`,
              [`header-b${i}`]: `header-value-b${i}`,
              [`header-c${i}`]: `header-value-c${i}`,
            },
          })),
        })

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '0',
          errorCode: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
          partition: 0,
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '10',
          errorCode: 0,
          logAppendTime: '-1',
          logStartOffset: '0',
          partition: 0,
        },
      ])
    })
  }

  testProduceMessages(false)

  describe('when idempotent=true', () => {
    testProduceMessages(true)

    test('throws an error if sending a message with acks != -1', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      producer = createProducer({ cluster, logger: newLogger(), idempotent: true })
      await producer.connect()

      await expect(
        producer.send({
          acks: 1,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        })
      ).rejects.toEqual(
        new KafkaJSNonRetriableError(
          "Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees"
        )
      )

      await expect(
        producer.send({
          acks: 0,
          topic: topicName,
          messages: new Array(10).fill().map((_, i) => ({
            key: `key-${i}`,
            value: `value-${i}`,
          })),
        })
      ).rejects.toEqual(
        new KafkaJSNonRetriableError(
          "Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees"
        )
      )
    })

    test('sets the default retry value to MAX_SAFE_INTEGER', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      producer = createProducer({ cluster, logger: newLogger(), idempotent: true })
      expect(retrySpy).toHaveBeenCalledWith({ retries: Number.MAX_SAFE_INTEGER })
    })

    test('throws an error if retries < 1', async () => {
      expect(() =>
        createProducer({
          cluster: {},
          logger: newLogger(),
          idempotent: true,
          retry: { retries: 0 },
        })
      ).toThrowError(
        new KafkaJSNonRetriableError(
          'Idempotent producer must allow retries to protect against transient errors'
        )
      )
    })

    test('only calls initProducerId if unitialized', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      producer = createProducer({ cluster, logger: newLogger(), idempotent: true })

      await producer.connect()
      expect(initProducerIdSpy).toHaveBeenCalledTimes(1)

      initProducerIdSpy.mockClear()
      await producer.connect()
      expect(initProducerIdSpy).toHaveBeenCalledTimes(0)
    })
  })

  describe('transactions', () => {
    let transactionalId

    beforeEach(() => {
      transactionalId = `transactional-id-${secureRandom()}`
    })

    const testTransactionEnd = (shouldCommit = true) => {
      const endFn = shouldCommit ? 'commit' : 'abort'
      testIfKafkaAtLeast_0_11(`transaction flow ${endFn}`, async () => {
        const cluster = createCluster({
          createPartitioner: createModPartitioner,
        })

        await createTopic({ topic: topicName })

        producer = createProducer({
          cluster,
          logger: newLogger(),
          transactionalId,
        })

        await producer.connect()
        const txn = await producer.transaction()
        await expect(producer.transaction()).rejects.toEqual(
          new KafkaJSNonRetriableError(
            'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
          )
        )

        await txn.send({
          topic: topicName,
          messages: [{ key: '2', value: '2' }],
        })
        await txn.sendBatch({
          topicMessages: [
            {
              topic: topicName,
              messages: [{ key: '2', value: '2' }],
            },
          ],
        })

        await txn[endFn]() // Dynamic
        await expect(txn.send()).rejects.toEqual(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
        await expect(txn.sendBatch()).rejects.toEqual(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
        await expect(txn.commit()).rejects.toEqual(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
        await expect(txn.abort()).rejects.toEqual(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )

        expect(await producer.transaction()).toBeTruthy() // Can create another transaction
      })
    }

    testTransactionEnd(true)
    testTransactionEnd(false)

    testIfKafkaAtLeast_0_11('allows sending messages outside a transaction', async () => {
      const cluster = createCluster({
        createPartitioner: createModPartitioner,
      })

      await createTopic({ topic: topicName })

      producer = createProducer({
        cluster,
        logger: newLogger(),
        transactionalId,
      })

      await producer.connect()
      await producer.transaction()

      await producer.send({
        topic: topicName,
        messages: [
          {
            key: 'key',
            value: 'value',
          },
        ],
      })
      await producer.sendBatch({
        topicMessages: [
          {
            topic: topicName,
            messages: [
              {
                key: 'key',
                value: 'value',
              },
            ],
          },
        ],
      })
    })

    testIfKafkaAtLeast_0_11('supports sending offsets', async () => {
      const cluster = createCluster()

      const markOffsetAsCommittedSpy = jest.spyOn(cluster, 'markOffsetAsCommitted')

      await createTopic({ topic: topicName, partitions: 2 })

      producer = createProducer({
        cluster,
        logger: newLogger(),
        transactionalId,
      })

      await producer.connect()

      const consumerGroupId = `consumer-group-id-${secureRandom()}`
      const topics = [
        {
          topic: topicName,
          partitions: [
            {
              partition: 0,
              offset: '5',
            },
            {
              partition: 1,
              offset: '10',
            },
          ],
        },
      ]
      const txn = await producer.transaction()
      await txn.sendOffsets({ consumerGroupId, topics })

      expect(sendOffsetsSpy).toHaveBeenCalledWith({
        consumerGroupId,
        topics,
      })
      expect(markOffsetAsCommittedSpy).toHaveBeenCalledTimes(2)
      expect(markOffsetAsCommittedSpy.mock.calls[0][0]).toEqual({
        groupId: consumerGroupId,
        topic: topicName,
        partition: 0,
        offset: '5',
      })
      expect(markOffsetAsCommittedSpy.mock.calls[1][0]).toEqual({
        groupId: consumerGroupId,
        topic: topicName,
        partition: 1,
        offset: '10',
      })

      await txn.commit()

      const coordinator = await cluster.findGroupCoordinator({ groupId: consumerGroupId })
      const retry = createRetrier({ retries: 5 })

      // There is a potential delay between transaction commit and offset
      // commits propagating to all replicas - retry expecting initial failure.
      await retry(async () => {
        const { responses: consumerOffsets } = await coordinator.offsetFetch({
          groupId: consumerGroupId,
          topics,
        })

        expect(consumerOffsets).toEqual([
          {
            topic: topicName,
            partitions: [
              expect.objectContaining({
                offset: '5',
                partition: 0,
              }),
              expect.objectContaining({
                offset: '10',
                partition: 1,
              }),
            ],
          },
        ])
      })
    })

    describe('without operations', () => {
      testIfKafkaAtLeast_0_11('does not throw an error when aborting transaction', async () => {
        const cluster = createCluster({
          createPartitioner: createModPartitioner,
        })

        await createTopic({ topic: topicName })

        producer = createProducer({
          cluster,
          logger: newLogger(),
          transactionalId,
        })
        await producer.connect()

        const transaction = await producer.transaction()

        await expect(transaction.abort()).toResolve()
      })

      testIfKafkaAtLeast_0_11('does not throw an error when commiting transaction', async () => {
        const cluster = createCluster({
          createPartitioner: createModPartitioner,
        })

        await createTopic({ topic: topicName })

        producer = createProducer({
          cluster,
          logger: newLogger(),
          transactionalId,
        })
        await producer.connect()

        const transaction = await producer.transaction()

        await expect(transaction.commit()).toResolve()
      })

      testIfKafkaAtLeast_0_11(
        'allows createing transaction when the previous was aborted without any operations made in it',
        async () => {
          const cluster = createCluster({
            createPartitioner: createModPartitioner,
          })

          await createTopic({ topic: topicName })

          producer = createProducer({
            cluster,
            logger: newLogger(),
            transactionalId,
          })
          await producer.connect()

          const transaction = await producer.transaction()

          await expect(transaction.abort()).toResolve()
          await expect(producer.transaction()).toResolve()
        }
      )

      testIfKafkaAtLeast_0_11(
        'allows createing transaction when the previous was commited without any operations made in it',
        async () => {
          const cluster = createCluster({
            createPartitioner: createModPartitioner,
          })

          await createTopic({ topic: topicName })

          producer = createProducer({
            cluster,
            logger: newLogger(),
            transactionalId,
          })
          await producer.connect()

          const transaction = await producer.transaction()

          await expect(transaction.commit()).toResolve()
          await expect(producer.transaction()).toResolve()
        }
      )

      testIfKafkaAtLeast_0_11(
        "transaction that is created after a transaction that hasn't made any operations should work",
        async () => {
          const partition = 0
          const retry = createRetrier({ retries: 5 })
          const cluster = createCluster({
            createPartitioner: createModPartitioner,
          })

          await createTopic({ topic: topicName })

          producer = createProducer({
            cluster,
            logger: newLogger(),
            transactionalId,
          })
          await producer.connect()

          const invalidTransaction = await producer.transaction()
          await invalidTransaction.abort()

          const transaction = await producer.transaction()
          await transaction.send({
            topic: topicName,
            messages: [
              {
                value: 'value',
                partition,
              },
              {
                value: 'value',
                partition,
              },
            ],
          })

          await transaction.commit()

          await retry(async () => {
            const [topicOffset] = await cluster.fetchTopicsOffset([
              { topic: topicName, partitions: [{ partition }] },
            ])

            expect(topicOffset).toEqual({
              topic: topicName,
              partitions: expect.arrayContaining([
                {
                  partition,
                  offset: '3',
                },
              ]),
            })
          })
        }
      )
    })
  })
})
