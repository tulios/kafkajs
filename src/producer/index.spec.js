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
  testIfKafka011,
  createTopic,
} = require('testHelpers')

const { KafkaJSSASLAuthenticationError } = require('../errors')

describe('Producer', () => {
  let topicName, producer

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    await producer.disconnect()
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
    await expect(producer.connect()).rejects.toEqual(
      new KafkaJSSASLAuthenticationError(
        'SASL PLAIN authentication failed: Connection closed by the server'
      )
    )
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
    await expect(producer.connect()).rejects.toEqual(
      new KafkaJSSASLAuthenticationError(
        'SASL SCRAM SHA256 authentication failed: Connection closed by the server'
      )
    )
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
    await expect(producer.connect()).rejects.toEqual(
      new KafkaJSSASLAuthenticationError(
        'SASL SCRAM SHA512 authentication failed: Connection closed by the server'
      )
    )
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

  test('produce messages', async () => {
    const cluster = createCluster(
      Object.assign(connectionOpts(), {
        createPartitioner: createModPartitioner,
      })
    )

    await createTopic({ topic: topicName })

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const sendMessages = async () =>
      await producer.send({
        acks: 1,
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

  testIfKafka011('produce messages for Kafka 0.11', async () => {
    const cluster = createCluster(
      Object.assign(connectionOpts(), {
        allowExperimentalV011: true,
        createPartitioner: createModPartitioner,
      })
    )

    await createTopic({ topic: topicName })

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const sendMessages = async () =>
      await producer.send({
        acks: 1,
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

  testIfKafka011('produce messages for Kafka 0.11 with headers', async () => {
    const cluster = createCluster(
      Object.assign(connectionOpts(), {
        allowExperimentalV011: true,
        createPartitioner: createModPartitioner,
      })
    )

    await createTopic({ topic: topicName })

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const sendMessages = async () =>
      await producer.send({
        acks: 1,
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

  test('produce messages to multiple topics', async () => {
    const topics = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]

    await createTopic({ topic: topics[0] })
    await createTopic({ topic: topics[1] })

    const cluster = createCluster({
      ...connectionOpts(),
      createPartitioner: createModPartitioner,
    })
    const byTopicName = (a, b) => a.topicName.localeCompare(b.topicName)

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const sendBatch = async topics => {
      const topicMessages = topics.map(topic => ({
        acks: 1,
        topic,
        messages: new Array(10).fill().map((_, i) => ({
          key: `key-${i}`,
          value: `value-${i}`,
        })),
      }))

      return producer.sendBatch({
        acks: 1,
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
})
