const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')

const {
  createCluster,
  newLogger,
  createTopic,
  createModPartitioner,
  waitForMessages,
  secureRandom,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Admin', () => {
  describe('listGroups', () => {
    let admin, topicName, groupId, cluster, consumer, producer

    test('list groups', async () => {
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
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })

      admin = createAdmin({ cluster: cluster, logger: newLogger() })

      await admin.connect()

      jest.spyOn(cluster, 'refreshMetadataIfNecessary')

      await consumer.connect()
      await producer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []
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

      let foundGroupId = ''

      for (const group of listGroupResponse.groups) {
        if (group.groupId === groupId) foundGroupId = groupId
      }

      expect(foundGroupId).toEqual(groupId)

      await admin.disconnect()
      await consumer.disconnect()
      await producer.disconnect()
    })
  })
})
