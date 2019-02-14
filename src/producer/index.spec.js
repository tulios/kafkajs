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
  let spy = jest.fn().mockImplementation(jest.requireActual('../retry'))
  retrySpy = spy
  return spy
})

const InstrumentationEventEmitter = require('../instrumentation/emitter')
const createProducer = require('./index')
const {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  createCluster,
  createModPartitioner,
  sslBrokers,
  saslBrokers,
  newLogger,
  testIfKafka_0_11,
  createTopic,
} = require('testHelpers')
const createRetrier = require('../retry')

const { KafkaJSNonRetriableError } = require('../errors')

describe('Producer', () => {
  let topicName, producer

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
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

  test('throws an error for messages with keys that cannot be converted to buffers', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })

    await expect(
      producer.send({ acks: 1, topic: topicName, messages: [{ key: 1, value: 'value' }] })
    ).rejects.toThrow(
      `Invalid message for topic "${topicName}". "key" needs to be convertible to Buffer: {"key":1,"value":"value"}`
    )
  })

  test('throws an error for messages with values that cannot be converted to buffers', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })

    await expect(
      producer.send({ acks: 1, topic: topicName, messages: [{ value: 1 }] })
    ).rejects.toThrow(
      `Invalid message for topic "${topicName}". "value" needs to be convertible to Buffer: {"value":1}`
    )
  })

  test('allows messages with a null value to support tombstones', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await producer.send({ acks: 1, topic: topicName, messages: [{ foo: 'bar', value: null }] })
  })

  test('support SSL connections', async () => {
    const cluster = createCluster(sslConnectionOpts(), sslBrokers())
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  test('support SASL PLAIN connections', async () => {
    const cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'plain',
          username: 'test',
          password: 'testtest',
        },
      }),
      saslBrokers()
    )
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  test('support SASL SCRAM 256 connections', async () => {
    const cluster = createCluster(saslSCRAM256ConnectionOpts(), saslBrokers())
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  test('support SASL SCRAM 512 connections', async () => {
    const cluster = createCluster(saslSCRAM512ConnectionOpts(), saslBrokers())
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  test('throws an error if SASL PLAIN fails to authenticate', async () => {
    const cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'plain',
          username: 'wrong',
          password: 'wrong',
        },
      }),
      saslBrokers()
    )

    producer = createProducer({ cluster, logger: newLogger() })
    await expect(producer.connect()).rejects.toThrow(/SASL PLAIN authentication failed/)
  })

  test('throws an error if SASL SCRAM 256 fails to authenticate', async () => {
    const cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'SCRAM-SHA-256',
          username: 'wrong',
          password: 'wrong',
        },
      }),
      saslBrokers()
    )

    producer = createProducer({ cluster, logger: newLogger() })
    await expect(producer.connect()).rejects.toThrow(/SASL SCRAM SHA256 authentication failed/)
  })

  test('throws an error if SASL SCRAM 512 fails to authenticate', async () => {
    const cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'SCRAM-SHA-512',
          username: 'wrong',
          password: 'wrong',
        },
      }),
      saslBrokers()
    )

    producer = createProducer({ cluster, logger: newLogger() })
    await expect(producer.connect()).rejects.toThrow(/SASL SCRAM SHA512 authentication failed/)
  })

  test('reconnects the cluster if disconnected', async () => {
    const cluster = createCluster(
      Object.assign(connectionOpts(), {
        createPartitioner: createModPartitioner,
      })
    )

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
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          createPartitioner: createModPartitioner,
        })
      )

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
          errorCode: 0,
          offset: '0',
          partition: 0,
          timestamp: '-1',
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          topicName,
          errorCode: 0,
          offset: '10',
          partition: 0,
          timestamp: '-1',
        },
      ])
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
            topicName: topics[0],
            errorCode: 0,
            offset: '0',
            partition: 0,
            timestamp: '-1',
          },
          {
            topicName: topics[1],
            errorCode: 0,
            offset: '0',
            partition: 0,
            timestamp: '-1',
          },
        ].sort(byTopicName)
      )

      result = await sendBatch(topics)
      expect(result.sort(byTopicName)).toEqual(
        [
          {
            topicName: topics[0],
            errorCode: 0,
            offset: '10',
            partition: 0,
            timestamp: '-1',
          },
          {
            topicName: topics[1],
            errorCode: 0,
            offset: '10',
            partition: 0,
            timestamp: '-1',
          },
        ].sort(byTopicName)
      )
    })

    testIfKafka_0_11('produce messages for Kafka 0.11', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
          partition: 0,
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '10',
          errorCode: 0,
          logAppendTime: '-1',
          partition: 0,
        },
      ])
    })

    testIfKafka_0_11('produce messages for Kafka 0.11 without specifying message key', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
    })

    testIfKafka_0_11('produce messages for Kafka 0.11 with headers', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
          partition: 0,
        },
      ])

      expect(await sendMessages()).toEqual([
        {
          topicName,
          baseOffset: '10',
          errorCode: 0,
          logAppendTime: '-1',
          partition: 0,
        },
      ])
    })
  }

  testProduceMessages(false)

  describe('when idempotent=true', () => {
    testProduceMessages(true)

    test('throws an error if sending a message with acks != -1', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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
      testIfKafka_0_11(`transaction flow ${endFn}`, async () => {
        const cluster = createCluster(
          Object.assign(connectionOpts(), {
            allowExperimentalV011: true,
            createPartitioner: createModPartitioner,
          })
        )

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

    testIfKafka_0_11('allows sending messages outside a transaction', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
          createPartitioner: createModPartitioner,
        })
      )

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

    testIfKafka_0_11('supports sending offsets', async () => {
      const cluster = createCluster(
        Object.assign(connectionOpts(), {
          allowExperimentalV011: true,
        })
      )

      const markOffsetAsCommittedSpy = jest.spyOn(cluster, 'markOffsetAsCommitted')

      await createTopic({ topic: topicName })

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
  })
})
