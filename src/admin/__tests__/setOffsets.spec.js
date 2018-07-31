const createAdmin = require('../index')
const createConsumer = require('../../consumer')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

describe('Admin', () => {
  let topicName, groupId, admin, consumer

  const offsetFetch = async ({ cluster }) => {
    const coordinator = await cluster.findGroupCoordinator({ groupId })
    return coordinator.offsetFetch({
      groupId,
      topics: [
        {
          topic: topicName,
          partitions: [{ partition: 0 }],
        },
      ],
    })
  }

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
  })

  afterEach(async () => {
    await admin.disconnect()
    consumer && (await consumer.disconnect())
  })

  describe('createTopics', () => {
    test('throws an error if the groupId is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.setOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.setOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('throws an error if partitions is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.setOffsets({ groupId: 'groupId', topic: 'topic', partitions: null })
      ).rejects.toHaveProperty('message', 'Invalid partitions')

      await expect(
        admin.setOffsets({ groupId: 'groupId', topic: 'topic', partitions: [] })
      ).rejects.toHaveProperty('message', 'Invalid partitions')
    })

    test('set the consumer group to any offsets', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await offsetFetch({ cluster })
      expect(offsets).toEqual({
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '13', partition: 0 }],
            topic: topicName,
          },
        ],
      })
    })

    test('throws an error if the consumer group is runnig', async () => {
      consumer = createConsumer({ groupId, cluster: createCluster(), logger: newLogger() })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName })
      await consumer.run({ eachMessage: () => true })

      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.setOffsets({
          groupId,
          topic: topicName,
          partitions: [{ partition: 0, offset: 13 }],
        })
      ).rejects.toHaveProperty(
        'message',
        'The consumer group must have no running instances, current state: Stable'
      )
    })
  })
})
