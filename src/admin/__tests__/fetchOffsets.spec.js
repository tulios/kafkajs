const createAdmin = require('../index')
const { secureRandom, createCluster, newLogger, createTopic } = require('testHelpers')

describe('Admin', () => {
  let topicName, groupId, admin, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
  })

  describe('fetchOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.fetchOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.fetchOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('returns unresolved consumer group offsets', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '-1', metadata: null }])
    })

    test('returns the current consumer group offset', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
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
