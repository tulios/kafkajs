const createAdmin = require('../index')
const { secureRandom, createCluster, newLogger, createTopic } = require('testHelpers')

describe('Admin', () => {
  let admin, cluster, consumer, groupId, logger, topicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    logger = newLogger()
    cluster = createCluster()
    admin = createAdmin({ cluster, logger })

    await admin.connect()
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
  })

  describe('fetchOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      await expect(admin.fetchOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      await expect(admin.fetchOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('returns unresolved consumer group offsets', async () => {
      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '-1', metadata: null }])
    })

    test('returns the current consumer group offset', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '13', metadata: null }])
    })
  })
})
