const createProducer = require('../../producer')
const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('describe group', () => {
    it('returns the group description', async () => {
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      consumer.run({ eachMessage: jest.fn() })
      await waitForConsumerToJoinGroup(consumer)

      await expect(consumer.describeGroup()).resolves.toEqual({
        errorCode: 0,
        groupId,
        members: [
          {
            clientHost: expect.any(String),
            clientId: expect.any(String),
            memberId: expect.any(String),
            memberAssignment: expect.anything(),
            memberMetadata: expect.anything(),
          },
        ],
        protocol: 'RoundRobinAssigner',
        protocolType: 'consumer',
        state: 'Stable',
      })
    })
  })
})
