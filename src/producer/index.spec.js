const createProducer = require('./index')
const {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  createCluster,
  createModPartitioner,
  sslBrokers,
  saslBrokers,
  newLogger,
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

  test('reconnects the cluster if disconnected', async () => {
    const cluster = createCluster(
      Object.assign(connectionOpts(), {
        createPartitioner: createModPartitioner,
      })
    )

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
    await producer.send({
      topic: topicName,
      messages: [{ key: '1', value: '1' }],
    })

    expect(cluster.isConnected()).toEqual(true)
    await cluster.disconnect()
    expect(cluster.isConnected()).toEqual(false)

    await producer.send({
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

    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const sendMessages = async () =>
      await producer.send({
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
})
