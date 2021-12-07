const createAdmin = require('../index')

const { secureRandom, createCluster, newLogger, createTopic } = require('testHelpers')

describe('Admin', () => {
  let existingTopicName, numPartitions, admin, consumer

  beforeEach(async () => {
    existingTopicName = `test-topic-${secureRandom()}`
    numPartitions = 4

    await createTopic({ topic: existingTopicName, partitions: numPartitions })
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
  })

  describe('getTopicMetadata', () => {
    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.getTopicMetadata({ topics: [null] })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('retrieves metadata for each partition in the topic', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      const { topics: topicsMetadata } = await admin.getTopicMetadata({
        topics: [existingTopicName],
      })

      expect(topicsMetadata).toHaveLength(1)
      const topicMetadata = topicsMetadata[0]
      expect(topicMetadata).toHaveProperty('name', existingTopicName)
      expect(topicMetadata.partitions).toHaveLength(numPartitions)

      topicMetadata.partitions.forEach(partition => {
        expect(partition).toHaveProperty('partitionId')
        expect(partition).toHaveProperty('leader')
        expect(partition).toHaveProperty('replicas')
        expect(partition).toHaveProperty('partitionErrorCode')
        expect(partition).toHaveProperty('isr')
      })
    })

    test('by default retrieves metadata for all topics of which the cluster is aware', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      const { topics: topicsMetadataBeforeAware } = await admin.getTopicMetadata()
      expect(topicsMetadataBeforeAware).toHaveLength(0)

      await cluster.addTargetTopic(existingTopicName)

      const { topics: topicsMetadataAfterAware } = await admin.getTopicMetadata()
      expect(topicsMetadataAfterAware).toHaveLength(1)
      expect(topicsMetadataAfterAware[0]).toHaveProperty('name', existingTopicName)
    })

    test('creates a new topic if the topic does not exist and "allowAutoTopicCreation" is true', async () => {
      admin = createAdmin({
        cluster: createCluster({ allowAutoTopicCreation: true }),
        logger: newLogger(),
      })
      const newTopicName = `test-topic-${secureRandom()}`

      await admin.connect()
      const { topics: topicsMetadata } = await admin.getTopicMetadata({
        topics: [existingTopicName, newTopicName],
      })

      expect(topicsMetadata[0]).toHaveProperty('name', existingTopicName)
      expect(topicsMetadata[1]).toHaveProperty('name', newTopicName)
      expect(topicsMetadata[1].partitions).toHaveLength(1)
    })

    test('throws an error if the topic does not exist and "allowAutoTopicCreation" is false', async () => {
      admin = createAdmin({
        cluster: createCluster({ allowAutoTopicCreation: false }),
        logger: newLogger(),
      })
      const newTopicName = `test-topic-${secureRandom()}`

      await admin.connect()
      await expect(
        admin.getTopicMetadata({
          topics: [existingTopicName, newTopicName],
        })
      ).rejects.toHaveProperty(
        'message',
        `Failed to add target topic ${newTopicName}: This server does not host this topic-partition`
      )
    })
  })
})
