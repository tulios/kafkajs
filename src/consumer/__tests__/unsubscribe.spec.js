const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  waitForMessages,
  createModPartitioner,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

const createConsumer = require('../index')
const createProducer = require('../../producer')

describe('Consumer', () => {
  let groupId, cluster, consumer, producer

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`

    cluster = createCluster()
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      logger: newLogger(),
    })

    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('when unsubscribing to a topic', () => {
    it('removes the topic from the subscription list', async () => {
      const testScope = secureRandom()
      const regexMatchingTopic = `pattern-${testScope}-regex-${secureRandom()}`
      const topics = [`topic-${secureRandom()}`, `topic-${secureRandom()}`, regexMatchingTopic]

      await Promise.all(topics.map(topic => createTopic({ topic })))

      const messagesConsumed = []
      await consumer.connect()
      await consumer.subscribe({
        topics: [topics[0], topics[1], new RegExp(`pattern-${testScope}-regex-.*`, 'i')],
        fromBeginning: true,
      })

      await consumer.unsubscribe(topics[0])

      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      await waitForConsumerToJoinGroup(consumer)

      await producer.connect()
      await producer.sendBatch({
        acks: 1,
        topicMessages: [
          { topic: topics[0], messages: [{ key: 'drink', value: 'drink' }] },
          { topic: topics[1], messages: [{ key: 'your', value: 'your' }] },
          { topic: topics[2], messages: [{ key: 'ovaltine', value: 'ovaltine' }] },
        ],
      })

      await waitForMessages(messagesConsumed, { number: 2 })
      expect(messagesConsumed.map(m => m.message.value.toString())).toEqual(
        expect.arrayContaining(['your', 'ovaltine'])
      )
    })

    it('does nothing if the topic is not subscribed', async () => {
      const testScope = secureRandom()
      const regexMatchingTopic = `pattern-${testScope}-regex-${secureRandom()}`
      const topics = [`topic-${secureRandom()}`, `topic-${secureRandom()}`, regexMatchingTopic]

      await Promise.all(topics.map(topic => createTopic({ topic })))

      const messagesConsumed = []
      await consumer.connect()
      await consumer.subscribe({
        topics: [topics[0], topics[1], new RegExp(`pattern-${testScope}-regex-.*`, 'i')],
        fromBeginning: true,
      })

      await consumer.unsubscribe('not-a-topic-name')

      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      await waitForConsumerToJoinGroup(consumer)

      await producer.connect()
      await producer.sendBatch({
        acks: 1,
        topicMessages: [
          { topic: topics[0], messages: [{ key: 'drink', value: 'drink' }] },
          { topic: topics[1], messages: [{ key: 'your', value: 'your' }] },
          { topic: topics[2], messages: [{ key: 'ovaltine', value: 'ovaltine' }] },
        ],
      })

      await waitForMessages(messagesConsumed, { number: 3 })
      expect(messagesConsumed.map(m => m.message.value.toString())).toEqual(
        expect.arrayContaining(['drink', 'your', 'ovaltine'])
      )
    })
  })
})
