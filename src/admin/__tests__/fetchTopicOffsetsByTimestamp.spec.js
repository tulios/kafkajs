const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')

const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  createModPartitioner,
  waitFor,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Admin', () => {
  let topicName, admin, producer, cluster, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    await createTopic({ topic: topicName })

    admin = createAdmin({ cluster, logger: newLogger() })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })
  })
  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('fetchTopicOffsetsByTimestamp', () => {
    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.fetchTopicOffsetsByTimestamp(null)).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    const sendMessages = async n => {
      await admin.connect()
      await producer.connect()

      const messages = Array(n)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await producer.send({ acks: 1, topic: topicName, messages })
    }

    test('returns the offsets from timestamp', async () => {
      await sendMessages(10)
      const fromTimestamp = Date.now()
      await sendMessages(10)
      const futureTimestamp = Date.now()
      const offsetsFromTimestamp = await admin.fetchTopicOffsetsByTimestamp(
        topicName,
        fromTimestamp
      )
      expect(offsetsFromTimestamp).toEqual([{ partition: 0, offset: '10' }])
      const offsetsFutureTimestamp = await admin.fetchTopicOffsetsByTimestamp(
        topicName,
        futureTimestamp
      )
      expect(offsetsFutureTimestamp).toEqual([{ partition: 0, offset: '20' }])
      const groupId = `consumer-group-id-${secureRandom()}`
      consumer = createConsumer({
        cluster,
        groupId,
        maxWaitTimeInMs: 1,
        maxBytesPerPartition: 180,
        logger: newLogger(),
      })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      /** real timestamp in messages after `fromTimestamp` */
      let realTimestamp = 0
      consumer.run({
        eachMessage: async ({ message }) => {
          if (message.timestamp < fromTimestamp) return
          if (realTimestamp === 0) realTimestamp = message.timestamp
          if (realTimestamp > 0) consumer.stop()
        },
      })
      await waitForConsumerToJoinGroup(consumer)
      await waitFor(() => realTimestamp > 0)
      const offsetsRealTimestamp = await admin.fetchTopicOffsetsByTimestamp(
        topicName,
        realTimestamp
      )
      expect(offsetsRealTimestamp).toEqual([{ partition: 0, offset: '10' }])
    })

    test('returns the offsets from timestamp when no messages', async () => {
      const fromTimestamp = Date.now()
      const offsetsFromTimestamp = await admin.fetchTopicOffsetsByTimestamp(
        topicName,
        fromTimestamp
      )
      expect(offsetsFromTimestamp).toEqual([{ partition: 0, offset: '0' }])
      const offsets = await admin.fetchTopicOffsets(topicName)
      expect(offsets).toEqual([{ partition: 0, offset: '0', low: '0', high: '0' }])
    })
  })
})
