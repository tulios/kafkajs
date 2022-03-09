const createAdmin = require('../index')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

const NOT_CONTROLLER = 41

describe('Admin', () => {
  let topicName, admin

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('createPartitions', () => {
    test('throws an error if the topics array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.createPartitions({ topicPartitions: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic partitions array null'
      )

      await expect(
        admin.createPartitions({ topicPartitions: 'this-is-not-an-array' })
      ).rejects.toHaveProperty('message', 'Invalid topic partitions array this-is-not-an-array')
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.createPartitions({ topicPartitions: [{ topic: 123 }] })
      ).rejects.toHaveProperty(
        'message',
        'Invalid topic partitions array, the topic names have to be a valid string'
      )
    })

    test('throws an error if there are multiple entries for the same topic', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const topicPartitions = [{ topic: 'topic-123' }, { topic: 'topic-123' }]
      await expect(admin.createPartitions({ topicPartitions })).rejects.toHaveProperty(
        'message',
        'Invalid topic partitions array, it cannot have multiple entries for the same topic'
      )
    })

    test('throws an error trying to create partition if topic does not exists', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()

      await expect(
        admin.createPartitions({
          topicPartitions: [{ topic: topicName + 'x', count: 3 }],
        })
      ).rejects.toHaveProperty('message', 'This server does not host this topic-partition')
    })

    test('throws an error trying to assign invalid broker', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()

      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName }],
        })
      ).resolves.toEqual(true)

      await expect(
        admin.createPartitions({
          topicPartitions: [{ topic: topicName, count: 3, assignments: [[10]] }],
        })
      ).rejects.toHaveProperty('message', 'Replica assignment is invalid')
    })

    test('throws an error trying when total partition is less then current', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()

      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName, numPartitions: 3 }],
        })
      ).resolves.toEqual(true)

      await expect(
        admin.createPartitions({
          topicPartitions: [{ topic: topicName, count: 2 }],
        })
      ).rejects.toHaveProperty('message', 'Number of partitions is invalid')
    })

    test('create new partition', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName }],
        })
      ).resolves.toEqual(true)

      await expect(
        admin.createPartitions({
          topicPartitions: [{ topic: topicName, count: 3 }],
        })
      ).resolves.not.toThrow()
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const broker = { createPartitions: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.createPartitions({
          topicPartitions: [{ topic: topicName, count: 3 }],
        })
      ).resolves.not.toThrow()

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.createPartitions).toHaveBeenCalledTimes(1)
    })
  })
})
