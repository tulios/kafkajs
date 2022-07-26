const createAdmin = require('../index')
const {
  KafkaJSProtocolError,
  KafkaJSAggregateError,
  KafkaJSCreateTopicError,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

const NOT_CONTROLLER = 41
const TOPIC_ALREADY_EXISTS = 36
const INVALID_TOPIC_EXCEPTION = 17

describe('Admin', () => {
  let topicName, admin

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('createTopics', () => {
    test('throws an error if the topics array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.createTopics({ topics: null })).rejects.toHaveProperty(
        'message',
        'Invalid topics array null'
      )

      await expect(admin.createTopics({ topics: 'this-is-not-an-array' })).rejects.toHaveProperty(
        'message',
        'Invalid topics array this-is-not-an-array'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.createTopics({ topics: [{ topic: 123 }] })).rejects.toHaveProperty(
        'message',
        'Invalid topics array, the topic names have to be a valid string'
      )
    })

    test('throws an error if there are multiple entries for the same topic', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const topics = [{ topic: 'topic-123' }, { topic: 'topic-123' }]
      await expect(admin.createTopics({ topics })).rejects.toHaveProperty(
        'message',
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    })

    test.each([
      [
        'are not an array',
        'this-is-not-an-array',
        'Invalid configEntries for topic "topic-123", must be an array',
      ],
      [
        'contain a non-object',
        ['this-is-not-an-object'],
        'Invalid configEntries for topic "topic-123". Entry 0 must be an object',
      ],
      [
        'contain an entry with missing value property',
        [{ name: 'missing-value' }],
        'Invalid configEntries for topic "topic-123". Entry 0 must have a valid "value" property',
      ],
      [
        'contain an entry with missing name property',
        [{ value: 'missing-name' }],
        'Invalid configEntries for topic "topic-123". Entry 0 must have a valid "name" property',
      ],
    ])('throws an error if the config entries %s', async (_, configEntries, errorMessage) => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const topics = [{ topic: 'topic-123', configEntries }]
      await expect(admin.createTopics({ topics })).rejects.toHaveProperty('message', errorMessage)
    })

    test('create the new topics and return true', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName }],
        })
      ).resolves.toEqual(true)
    })

    test('creating topic with manual replica assignment', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName, replicaAssignment: [{ partition: 0, replicas: [0, 1] }] }],
        })
      ).resolves.toEqual(true)
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const broker = { createTopics: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName }],
        })
      ).resolves.toEqual(true)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.createTopics).toHaveBeenCalledTimes(1)
    })

    test('ignore already created topics and return false', async () => {
      const cluster = createCluster()
      const broker = { createTopics: jest.fn() }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest.fn(() => broker)
      broker.createTopics.mockImplementationOnce(() => {
        throw new KafkaJSAggregateError('error', [
          new KafkaJSCreateTopicError(createErrorFromCode(TOPIC_ALREADY_EXISTS), topicName),
        ])
      })

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: topicName }],
        })
      ).resolves.toEqual(false)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(1)
      expect(broker.createTopics).toHaveBeenCalledTimes(1)
    })

    test('query metadata if waitForLeaders is true', async () => {
      const topic2 = `test-topic-${secureRandom()}`
      const topic3 = `test-topic-${secureRandom()}`

      const cluster = createCluster()
      const broker = { createTopics: jest.fn(), metadata: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest.fn(() => broker)

      broker.createTopics.mockImplementationOnce(() => true)
      admin = createAdmin({ cluster, logger: newLogger() })

      await expect(
        admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: topicName }, { topic: topic2 }, { topic: topic3 }],
        })
      ).resolves.toEqual(true)

      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(broker.metadata).toHaveBeenCalledWith([topicName, topic2, topic3])
    })

    test('forward non ignorable errors with topic name metadata', async () => {
      const cluster = createCluster()
      const broker = { createTopics: jest.fn(), metadata: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest.fn(() => broker)

      broker.createTopics.mockImplementationOnce(() => {
        throw new KafkaJSAggregateError('error', [
          new KafkaJSCreateTopicError(createErrorFromCode(INVALID_TOPIC_EXCEPTION), topicName),
        ])
      })
      admin = createAdmin({ cluster, logger: newLogger() })

      await expect(
        admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: topicName }],
        })
      ).rejects.toBeInstanceOf(KafkaJSAggregateError)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(1)
      expect(broker.createTopics).toHaveBeenCalledTimes(1)
    })
  })
})
