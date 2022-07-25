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

  describe('listPartitionReassignments', () => {
    test('throws an error if the topics array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await expect(
        admin.alterPartitionReassignments({ topics: 'this-is-not-an-array' })
      ).rejects.toHaveProperty('message', 'Invalid topics array this-is-not-an-array')
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.listPartitionReassignments({ topics: [{ topic: 123 }] })
      ).rejects.toHaveProperty(
        'message',
        'Invalid topics array, the topic names have to be a valid string'
      )
    })

    test('throws an error if there are multiple entries for the same topic', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const topics = [{ topic: 'topic-123' }, { topic: 'topic-123' }]
      await expect(admin.listPartitionReassignments({ topics })).rejects.toHaveProperty(
        'message',
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    })

    test('throws an error if the partition array is not a valid array', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.listPartitionReassignments({ topics: [{ topic: 'topic-123', partitions: null }] })
      ).rejects.toHaveProperty('message', 'Invalid partition array: null for topic: topic-123')

      await expect(
        admin.listPartitionReassignments({
          topics: [{ topic: 'topic-123', partitions: 'this-is-not-an-array' }],
        })
      ).rejects.toHaveProperty(
        'message',
        'Invalid partition array: this-is-not-an-array for topic: topic-123'
      )
    })

    test('throws an error if the partitions are not a valid number', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.listPartitionReassignments({ topics: [{ topic: 'topic-123', partitions: [0, 'a'] }] })
      ).rejects.toHaveProperty(
        'message',
        'Invalid partition array: 0,a for topic: topic-123. The partition indices have to be a valid number greater than 0.'
      )
    })

    test('list reassignments', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await expect(
        admin.createTopics({
          topics: [
            {
              topic: topicName,
              replicaAssignment: [
                { partition: 0, replicas: [1, 0] },
                { partition: 1, replicas: [2, 1] },
              ],
            },
          ],
        })
      ).resolves.toEqual(true)

      await admin.alterPartitionReassignments({
        topics: [{ topic: topicName, partitionAssignment: [{ partition: 0, replicas: [2, 1] }] }],
      })

      await expect(
        admin.listPartitionReassignments({
          topics: [{ topic: topicName, partitions: [0, 1, 2] }],
        })
      ).resolves.not.toThrow()
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const broker = { listPartitionReassignments: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.listPartitionReassignments({
          topics: [{ topic: topicName, partitions: [0, 1, 2] }],
        })
      ).resolves.not.toThrow()

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.listPartitionReassignments).toHaveBeenCalledTimes(1)
    })
  })
})
