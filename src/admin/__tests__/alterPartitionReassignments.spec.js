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

  describe('alterPartitionReassignments', () => {
    test('throws an error if the topics array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.alterPartitionReassignments({ topics: null })).rejects.toHaveProperty(
        'message',
        'Invalid topics array null'
      )

      await expect(
        admin.alterPartitionReassignments({ topics: 'this-is-not-an-array' })
      ).rejects.toHaveProperty('message', 'Invalid topics array this-is-not-an-array')
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.alterPartitionReassignments({ topics: [{ topic: 123 }] })
      ).rejects.toHaveProperty(
        'message',
        'Invalid topics array, the topic names have to be a valid string'
      )
    })

    test('throws an error if there are multiple entries for the same topic', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const topics = [{ topic: 'topic-123' }, { topic: 'topic-123' }]
      await expect(admin.alterPartitionReassignments({ topics })).rejects.toHaveProperty(
        'message',
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    })

    test('throws an error if the partitionAssignment array is not valid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: 'topic-123', partitionAssignment: null }],
        })
      ).rejects.toHaveProperty('message', 'Invalid partitions array: null for topic: topic-123')

      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: 'topic-123', partitionAssignment: 'this-is-not-an-array' }],
        })
      ).rejects.toHaveProperty(
        'message',
        'Invalid partitions array: this-is-not-an-array for topic: topic-123'
      )
    })

    test('throws an error if the partition index is not valid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: 'topic-123', partitionAssignment: [{ partition: null }] }],
        })
      ).rejects.toHaveProperty('message', 'Invalid partitions index: null for topic: topic-123')

      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: 'topic-123', partitionAssignment: [{ partition: -1 }] }],
        })
      ).rejects.toHaveProperty('message', 'Invalid partitions index: -1 for topic: topic-123')
    })

    test('throws an error if the replicas array is not valid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: 'topic-123', partitionAssignment: [{ partition: 0, replicas: null }] }],
        })
      ).rejects.toHaveProperty(
        'message',
        'Invalid replica assignment: null for topic: topic-123 on partition: 0'
      )

      await expect(
        admin.alterPartitionReassignments({
          topics: [
            { topic: 'topic-123', partitionAssignment: [{ partition: 0, replicas: [0, 'a'] }] },
          ],
        })
      ).rejects.toHaveProperty(
        'message',
        'Invalid replica assignment: 0,a for topic: topic-123 on partition: 0. Replicas must be a non negative number'
      )
    })

    test('throws an error if the if trying to reassign partitions for a topic that does not exist', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()

      await expect(
        admin.alterPartitionReassignments({
          topics: [
            { topic: topicName + 'x', partitionAssignment: [{ partition: 0, replicas: [2, 1] }] },
          ],
        })
      ).rejects.toThrow(
        expect.objectContaining({
          message: 'Errors altering partition reassignments',
          errors: expect.arrayContaining([
            expect.objectContaining({
              message: 'This server does not host this topic-partition',
              topic: topicName + 'x',
              partition: 0,
            }),
          ]),
        })
      )
    })

    test('throws an error trying to assign invalid broker', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()

      await expect(
        admin.createTopics({
          waitForLeaders: false,
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

      await expect(
        admin.alterPartitionReassignments({
          topics: [
            {
              topic: topicName,
              partitionAssignment: [{ partition: 0, replicas: [5, 1] }],
            },
          ],
        })
      ).rejects.toThrow(
        expect.objectContaining({
          message: 'Errors altering partition reassignments',
          errors: expect.arrayContaining([
            expect.objectContaining({
              message: 'Replica assignment is invalid',
              topic: topicName,
              partition: 0,
            }),
          ]),
        })
      )
    })

    test('reassign partitions', async () => {
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

      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: topicName, partitionAssignment: [{ partition: 0, replicas: [2, 1] }] }],
        })
      ).resolves.not.toThrow()
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const broker = { alterPartitionReassignments: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.alterPartitionReassignments({
          topics: [{ topic: topicName, partitionAssignment: [{ partition: 0, replicas: [2, 1] }] }],
        })
      ).resolves.not.toThrow()

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
    })
  })
})
