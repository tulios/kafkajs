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
    await consumer.disconnect()
    await producer.disconnect()
  })

  describe('when seek offset', () => {
    it('throws an error if the topic is invalid', () => {
      expect(() => consumer.seek({ topic: null })).toThrow(
        KafkaJSNonRetriableError,
        'Invalid topic null'
      )
    })

    it('throws an error if the partition is not a number', () => {
      expect(() => consumer.seek({ topic: topicName, partition: 'ABC' })).toThrow(
        KafkaJSNonRetriableError,
        'Invalid partition, expected a number received ABC'
      )
    })

    it('throws an error if the offset is not a number', () => {
      expect(() => consumer.seek({ topic: topicName, partition: 0, offset: 'ABC' })).toThrow(
        KafkaJSNonRetriableError,
        'Invalid offset, expected a long received ABC'
      )
    })

    it('throws an error if the offset is negative', () => {
      expect(() => consumer.seek({ topic: topicName, partition: 0, offset: '-1' })).toThrow(
        KafkaJSNonRetriableError,
        'Offset must not be a negative number'
      )
    })

    it('throws an error if called before consumer run', () => {
      expect(() => consumer.seek({ topic: topicName, partition: 0, offset: '1' })).toThrow(
        KafkaJSNonRetriableError,
        'Consumer group was not initialized, consumer#run must be called first'
      )
    })

    it('updates the partition offset to the given offset', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      const key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ topic: topicName, messages: [message1, message2, message3] })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []
      // must be called after run because the ConsumerGroup must be initialized
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      consumer.seek({ topic: topicName, partition: 0, offset: 1 })

      await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
        {
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({ offset: '1' }),
        },
        {
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({ offset: '2' }),
        },
      ])
    })

    it('uses the last seek for a given topic/partition', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      const key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ topic: topicName, messages: [message1, message2, message3] })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      consumer.seek({ topic: topicName, partition: 0, offset: 0 })
      consumer.seek({ topic: topicName, partition: 0, offset: 1 })
      consumer.seek({ topic: topicName, partition: 0, offset: 2 })

      await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
        {
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({ offset: '2' }),
        },
      ])
    })

    it('recovers from offset out of range', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }

      await producer.send({ topic: topicName, messages: [message1] })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      consumer.seek({ topic: topicName, partition: 0, offset: 100 })

      await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
        {
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({ offset: '0' }),
        },
      ])
    })
  })
})
