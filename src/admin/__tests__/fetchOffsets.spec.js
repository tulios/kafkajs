const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  createModPartitioner,
  waitForConsumerToJoinGroup,
  generateMessages,
  waitForMessages,
} = require('testHelpers')

describe('Admin', () => {
  let admin, cluster, groupId, logger, topicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    logger = newLogger()
    cluster = createCluster()
    admin = createAdmin({ cluster, logger })

    await admin.connect()
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('fetchOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      await expect(admin.fetchOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      await expect(admin.fetchOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('returns unresolved consumer group offsets', async () => {
      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '-1', metadata: null }])
    })

    test('returns the current consumer group offset', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '13', metadata: null }])
    })

    describe('when resolveOffsets option is enabled', () => {
      let producer, consumer, messagesConsumed

      beforeEach(async () => {
        producer = createProducer({
          cluster,
          createPartitioner: createModPartitioner,
          logger,
        })
        await producer.connect()

        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 100,
          logger,
        })
        await consumer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        await waitForConsumerToJoinGroup(consumer)

        const messages = generateMessages({ number: 10 })
        await producer.send({
          acks: 1,
          topic: topicName,
          messages,
        })
        await waitForMessages(messagesConsumed, { number: messages.length })
        await consumer.stop()
      })

      afterEach(async () => {
        producer && (await producer.disconnect())
        consumer && (await consumer.disconnect())
      })

      test('returns latest topic offsets after resolving, and persists them', async () => {
        await admin.resetOffsets({ groupId, topic: topicName })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })

        expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '-1', metadata: null }])
        expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '10', metadata: null }])
        expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '10', metadata: null }])
      })

      test('returns earliest topic offsets after resolving, and persists them', async () => {
        await admin.resetOffsets({ groupId, topic: topicName, earliest: true })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })

        expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '-2', metadata: null }])
        expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '0', metadata: null }])
        expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '0', metadata: null }])
      })
    })
  })
})
