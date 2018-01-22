const createProducer = require('../producer')
const createConsumer = require('./index')
const { LEVELS: { DEBUG } } = require('../loggers')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  sslConnectionOpts,
  sslBrokers,
  saslBrokers,
  waitFor,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
  })

  afterEach(async () => {
    await consumer.disconnect()
  })

  test('support SSL connections', async () => {
    cluster = createCluster(sslConnectionOpts(), sslBrokers())
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })

    await consumer.connect()
  })

  test('support SASL PLAIN connections', async () => {
    cluster = createCluster(
      Object.assign(sslConnectionOpts(), {
        sasl: {
          mechanism: 'plain',
          username: 'test',
          password: 'testtest',
        },
      }),
      saslBrokers()
    )

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })

    await consumer.connect()
  })

  test('reconnects the cluster if disconnected', async () => {
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      logger: newLogger({ level: DEBUG }),
      retry: { retries: 3 },
    })

    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    let messages = []
    await consumer.run({
      eachMessage: async ({ message }) => {
        messages.push(message)
      },
    })

    expect(cluster.isConnected()).toEqual(true)
    await cluster.disconnect()
    expect(cluster.isConnected()).toEqual(false)

    try {
      await producer.send({
        topic: topicName,
        messages: [{ key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }],
      })
    } finally {
      await producer.disconnect()
    }

    await waitFor(() => cluster.isConnected())
    await expect(waitFor(() => messages.length > 0)).resolves.toBeTruthy()
  })
})
