const createAdmin = require('../index')
const createConsumer = require('../../consumer')
const createProducer = require('../../producer')

const {
  createCluster,
  newLogger,
  createTopic,
  secureRandom,
  createModPartitioner,
  waitForConsumerToJoinGroup,
  waitForMessages,
} = require('testHelpers')

describe('Admin', () => {
  let admin, topicName, groupId, cluster, consumer, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    cluster = createCluster()
    admin = createAdmin({ cluster: cluster, logger: newLogger() })
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    await Promise.all([admin.connect(), consumer.connect(), producer.connect()])
  })

  afterEach(async () => {
    // Checking that they exist first, in case the test is skipped or failed before instantiating the admin/consumer
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('listGroups', () => {
    test('list groups', async () => {
      await createTopic({ topic: topicName })

      const messagesConsumed = []
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      await waitForConsumerToJoinGroup(consumer)

      const messages = Array(1)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await producer.send({ acks: 1, topic: topicName, messages })
      await waitForMessages(messagesConsumed, { number: messages.length })

      const listGroupResponse = await admin.listGroups()

      expect(listGroupResponse.groups).toEqual(
        expect.arrayContaining([expect.objectContaining({ groupId })])
      )
    })
  })
})
