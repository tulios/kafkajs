const createAdmin = require('../index')
const createProducer = require('../../producer')
const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  createModPartitioner,
} = require('testHelpers')

describe('Admin', () => {
  let topicName, admin, producer, cluster

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
    producer && (await producer.disconnect())
  })

  describe('fetchTopicOffsets', () => {
    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.fetchTopicOffsets(null)).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('returns the current topic offset', async () => {
      await admin.connect()
      await producer.connect()

      const messages = Array(100)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await producer.send({ acks: 1, topic: topicName, messages })
      const offsets = await admin.fetchTopicOffsets(topicName)

      expect(offsets).toEqual([{ partition: 0, offset: '100', low: '0', high: '100' }])
    })
  })
})
