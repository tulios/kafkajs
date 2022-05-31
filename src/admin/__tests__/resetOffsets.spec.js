const createAdmin = require('../index')
const createConsumer = require('../../consumer')

const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

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

  describe('resetOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.resetOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.resetOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('set the consumer group offsets to the latest offsets', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      await admin.resetOffsets({
        groupId,
        topic: topicName,
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topicName],
      })

      expect(offsets).toEqual([
        { topic: topicName, partitions: [{ partition: 0, offset: '-1', metadata: null }] },
      ])
    })

    test('set the consumer group offsets to the earliest offsets', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      await admin.resetOffsets({
        groupId,
        topic: topicName,
        earliest: true,
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topicName],
      })

      expect(offsets).toEqual([
        { topic: topicName, partitions: [{ partition: 0, offset: '-2', metadata: null }] },
      ])
    })

    test('throws an error if the consumer group is running', async () => {
      consumer = createConsumer({ groupId, cluster: createCluster(), logger: newLogger() })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName })
      consumer.run({ eachMessage: () => true })
      await waitForConsumerToJoinGroup(consumer)

      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await expect(
        admin.resetOffsets({
          groupId,
          topic: topicName,
        })
      ).rejects.toHaveProperty(
        'message',
        'The consumer group must have no running instances, current state: Stable'
      )
    })
  })
})
