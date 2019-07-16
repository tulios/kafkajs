const createProducer = require('../../producer')
const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
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
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('when commitOffsets', () => {
    it('throws an error if any of the topics is invalid', () => {
      expect(() => consumer.commitOffsets([{ topic: null }])).toThrow(
        KafkaJSNonRetriableError,
        'Invalid topic null'
      )
    })

    it('throws an error if anyof the partitions is not a number', () => {
      expect(() => consumer.commitOffsets([{ topic: topicName, partition: 'ABC' }])).toThrow(
        KafkaJSNonRetriableError,
        'Invalid partition, expected a number received ABC'
      )
    })

    it('throws an error if any of the offsets is not a number', () => {
      expect(() =>
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: 'ABC' }])
      ).toThrow(KafkaJSNonRetriableError, 'Invalid offset, expected a long received ABC')
    })

    it('throws an error if any of the offsets is not an absolute offset', () => {
      expect(() =>
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '-1' }])
      ).toThrow(KafkaJSNonRetriableError, 'Offset must not be a negative number')
    })

    it('throws an error if called before consumer run', () => {
      expect(() =>
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '1' }])
      ).toThrow(
        KafkaJSNonRetriableError,
        'Consumer group was not initialized, consumer#run must be called first'
      )
    })

    it('updates the partition committed offset to the given offset', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      const key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ acks: 1, topic: topicName, messages: [message1, message2, message3] })

      const messagesConsumed = []

      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      consumer.run({
        autoCommit: false,
        eachMessage: async event => {
          messagesConsumed.push(event)
          if (messagesConsumed.length >= 3) {
            consumer.commitOffset([
              { topic: topicName, partition: 0, offset: parseInt(event.offset) + 1 },
            ])
          }
        },
      })

      await waitForConsumerToJoinGroup(consumer)
      await waitForMessages(messagesConsumed, { number: 3 })
    })
  })
})
