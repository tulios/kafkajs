const createProducer = require('../../producer')
const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForMessages,
} = require('testHelpers')

describe('Consumer', () => {
  let groupId, cluster, producer, consumer, topics

  beforeEach(async () => {
    topics = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]
    groupId = `consumer-group-id-${secureRandom()}`

    for (let topic of topics) {
      await createTopic({ topic, partitions: 2 })
    }

    cluster = createCluster()
    producer = createProducer({
      cluster,
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
    await consumer.disconnect()
    await producer.disconnect()
  })

  describe('when pausing', () => {
    it('throws an error if the topic is invalid', () => {
      expect(() => consumer.pause([{ topic: null, partitions: [0] }])).toThrow(
        KafkaJSNonRetriableError,
        'Invalid topic null'
      )
    })

    it('throws an error if Consumer#run has not been called', () => {
      expect(() => consumer.pause([{ topic: 'foo', partitions: [0] }])).toThrow(
        KafkaJSNonRetriableError,
        'Consumer group was not initialized, consumer#run must be called first'
      )
    })

    it('does not fetch messages for the paused topic', async () => {
      await consumer.connect()
      await producer.connect()

      const key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}`, partition: 1 }

      for (let topic of topics) {
        await producer.send({ topic, messages: [message1] })
        await consumer.subscribe({ topic, fromBeginning: true })
      }

      const messagesConsumed = []
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

      await waitForMessages(messagesConsumed, { number: 2 })

      const [pausedTopic, activeTopic] = topics
      consumer.pause([{ topic: pausedTopic }])

      for (let topic of topics) {
        await producer.send({ topic, messages: [message2] })
      }

      const consumedMessages = await waitForMessages(messagesConsumed, { number: 3 })

      expect(consumedMessages.filter(({ topic }) => topic === pausedTopic)).toEqual([
        {
          topic: pausedTopic,
          partition: 0,
          message: expect.objectContaining({ offset: '0' }),
        },
      ])

      const byPartition = (a, b) => a.partition - b.partition
      expect(
        consumedMessages.filter(({ topic }) => topic === activeTopic).sort(byPartition)
      ).toEqual([
        {
          topic: activeTopic,
          partition: 0,
          message: expect.objectContaining({ offset: '0' }),
        },
        {
          topic: activeTopic,
          partition: 1,
          message: expect.objectContaining({ offset: '0' }),
        },
      ])
    })
  })

  describe('when resuming', () => {
    it('throws an error if the topic is invalid', () => {
      expect(() => consumer.pause([{ topic: null, partitions: [0] }])).toThrow(
        KafkaJSNonRetriableError,
        'Invalid topic null'
      )
    })

    it('throws an error if Consumer#run has not been called', () => {
      expect(() => consumer.pause([{ topic: 'foo', partitions: [0] }])).toThrow(
        KafkaJSNonRetriableError,
        'Consumer group was not initialized, consumer#run must be called first'
      )
    })

    it('resumes fetching from the specified topic', async () => {
      await consumer.connect()
      await producer.connect()

      const key = secureRandom()
      const message = { key: `key-${key}`, value: `value-${key}`, partition: 0 }

      for (let topic of topics) {
        await consumer.subscribe({ topic, fromBeginning: true })
      }

      const messagesConsumed = []
      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

      const [pausedTopic, activeTopic] = topics
      consumer.pause([{ topic: pausedTopic }])

      for (let topic of topics) {
        await producer.send({ topic, messages: [message] })
      }

      await waitForMessages(messagesConsumed, { number: 1 })

      consumer.resume([{ topic: pausedTopic }])

      await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
        {
          topic: activeTopic,
          partition: 0,
          message: expect.objectContaining({ offset: '0' }),
        },
        {
          topic: pausedTopic,
          partition: 0,
          message: expect.objectContaining({ offset: '0' }),
        },
      ])
    })
  })
})
