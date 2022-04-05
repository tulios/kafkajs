const createAdmin = require('../index')
const createConsumer = require('../../consumer')
const createProducer = require('../../producer')
const { KafkaJSProtocolError } = require('../../errors')

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

  afterEach(async () => {
    // Checking that they exist first, in case the test is skipped or failed before instantiating the admin/consumer
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('deleteGroups', () => {
    test('delete empty groups', async () => {
      // let's check deleting empty group
      const resEmpty = await admin.deleteGroups([])

      expect(resEmpty).toEqual([])
    })

    test('delete groups that is still connected', async () => {
      // let's try to delete group that has consumer connected, it should throw error
      try {
        await admin.deleteGroups([groupId])
      } catch (error) {
        expect(error.name).toEqual('KafkaJSDeleteGroupsError')
        expect(error.groups).toEqual(expect.arrayContaining([expect.objectContaining({ groupId })]))
        for (const group of error.groups) {
          expect(group.error).toBeInstanceOf(KafkaJSProtocolError)
        }
      }
    })

    test('delete groups', async () => {
      // now let's try to stop consumer and then delete group
      await consumer.stop()

      const res = await admin.deleteGroups([groupId])
      expect(res).toEqual(expect.arrayContaining([expect.objectContaining({ groupId })]))

      const listGroupResponseAfter = await admin.listGroups()

      expect(listGroupResponseAfter.groups).toEqual(
        expect.not.arrayContaining([expect.objectContaining({ groupId })])
      )
    })

    test('delete one group that does not exist and one that exists', async () => {
      // let's disconnect from consumer
      await consumer.stop()

      // let's try to delete group that exits and one that doesn't, it should throw error but delete the one that does
      const nonExistingGroupId = `consumer-group-id-${secureRandom()}`
      try {
        await admin.deleteGroups([groupId, nonExistingGroupId])
      } catch (error) {
        expect(error.name).toEqual('KafkaJSDeleteGroupsError')
        expect(error.groups).toEqual(
          expect.arrayContaining([expect.objectContaining({ groupId: nonExistingGroupId })])
        )
        for (const group of error.groups) {
          expect(group.error).toBeInstanceOf(KafkaJSProtocolError)
        }
      }

      const listGroupResponseAfter = await admin.listGroups()

      expect(listGroupResponseAfter.groups).toEqual(
        expect.not.arrayContaining([expect.objectContaining({ groupId })])
      )
    })
  })
})
