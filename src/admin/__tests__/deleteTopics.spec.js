const createAdmin = require('../index')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

const NOT_CONTROLLER = 41
const REQUEST_TIMED_OUT = 7

describe('Admin', () => {
  let topicName, admin

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('deleteTopics', () => {
    test('throws an error if the topics array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.deleteTopics({ topics: null })).rejects.toHaveProperty(
        'message',
        'Invalid topics array null'
      )

      await expect(admin.deleteTopics({ topics: 'this-is-not-an-array' })).rejects.toHaveProperty(
        'message',
        'Invalid topics array this-is-not-an-array'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.deleteTopics({ topics: [123] })).rejects.toHaveProperty(
        'message',
        'Invalid topics array, the names must be a valid string'
      )
    })

    test('delete topics', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic: topicName }] })
      await expect(admin.deleteTopics({ topics: [topicName] })).resolves.toBe()
    })

    test('remove deleted topics from the cluster target group', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()

      await expect(admin.fetchTopicOffsets(topicName)).resolves.toBeTruthy()
      expect(cluster.targetTopics.size).toEqual(1)

      await admin.deleteTopics({ topics: [topicName] })
      expect(cluster.targetTopics.size).toEqual(0)
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const broker = { deleteTopics: jest.fn(() => true) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(admin.deleteTopics({ topics: [topicName] })).resolves.toBe()

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(3)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
    })

    test('interrupts on REQUEST_TIMED_OUT', async () => {
      const cluster = createCluster()
      const broker = {
        deleteTopics: jest.fn(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
        }),
      }

      const loggerInstance = { error: jest.fn() }
      const logger = { namespace: jest.fn(() => loggerInstance) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest.fn(() => broker)

      admin = createAdmin({ cluster, logger })
      await expect(admin.deleteTopics({ topics: [topicName] })).rejects.toHaveProperty(
        'message',
        'The request timed out'
      )

      expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
      expect(
        loggerInstance.error
      ).toHaveBeenCalledWith(
        'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
        { error: 'The request timed out', retryCount: 0, retryTime: expect.any(Number) }
      )
    })
  })
})
