const createProducer = require('../../producer')
const createManualConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
} = require('testHelpers')

describe('ManualConsumer', () => {
  let topicName, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createManualConsumer({
      cluster,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('when seek offset', () => {
    describe('with one partition', () => {
      beforeEach(async () => {
        await createTopic({ topic: topicName, partitions: 1 })
      })

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

      it('throws an error if the offset is negative and not a special offset', () => {
        expect(() => consumer.seek({ topic: topicName, partition: 0, offset: '-32' })).toThrow(
          KafkaJSNonRetriableError,
          'Offset must not be a negative number'
        )
      })

      it('throws an error if called before consumer run', () => {
        expect(() => consumer.seek({ topic: topicName, partition: 0, offset: '1' })).toThrow(
          KafkaJSNonRetriableError,
          'Consumer was not initialized, consumer#run must be called first'
        )
      })

      it('recovers from offset out of range', async () => {
        await consumer.connect()
        await producer.connect()

        const key1 = secureRandom()
        const message1 = { key: `key-${key1}`, value: `value-${key1}` }

        await producer.send({ acks: 1, topic: topicName, messages: [message1] })
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 0, offset: 100 })

        await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
          expect.objectContaining({
            topic: topicName,
            partition: 0,
            message: expect.objectContaining({ offset: '0' }),
          }),
        ])
      })
    })

    describe('with two partitions', () => {
      beforeEach(async () => {
        await createTopic({ topic: topicName, partitions: 2 })
      })

      it('updates the partition offset to the given offset', async () => {
        await consumer.connect()
        await producer.connect()

        const value1 = secureRandom()
        const message1 = { key: `key-1`, value: `value-${value1}` }
        const value2 = secureRandom()
        const message2 = { key: `key-1`, value: `value-${value2}` }
        const value3 = secureRandom()
        const message3 = { key: `key-1`, value: `value-${value3}` }
        const value4 = secureRandom()
        const message4 = { key: `key-0`, value: `value-${value4}` }

        await producer.send({
          acks: 1,
          topic: topicName,
          messages: [message1, message2, message3, message4],
        })
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 1, offset: 1 })

        await expect(waitForMessages(messagesConsumed, { number: 3 })).resolves.toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '0' }),
            }),
            expect.objectContaining({
              topic: topicName,
              partition: 1,
              message: expect.objectContaining({ offset: '1' }),
            }),
            expect.objectContaining({
              topic: topicName,
              partition: 1,
              message: expect.objectContaining({ offset: '2' }),
            }),
          ])
        )
      })

      it('works for both partitions', async () => {
        await consumer.connect()
        await producer.connect()

        const value1 = secureRandom()
        const message1 = { key: `key-1`, value: `value-${value1}` }
        const value2 = secureRandom()
        const message2 = { key: `key-1`, value: `value-${value2}` }
        const value3 = secureRandom()
        const message3 = { key: `key-0`, value: `value-${value3}` }
        const value4 = secureRandom()
        const message4 = { key: `key-0`, value: `value-${value4}` }
        const value5 = secureRandom()
        const message5 = { key: `key-0`, value: `value-${value5}` }

        await producer.send({
          acks: 1,
          topic: topicName,
          messages: [message1, message2, message3, message4, message5],
        })
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 0, offset: 2 })
        consumer.seek({ topic: topicName, partition: 1, offset: 1 })

        await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '2' }),
            }),
            expect.objectContaining({
              topic: topicName,
              partition: 1,
              message: expect.objectContaining({ offset: '1' }),
            }),
          ])
        )
      })

      it('uses the last seek for a given topic/partition', async () => {
        await consumer.connect()
        await producer.connect()

        const value1 = secureRandom()
        const message1 = { key: `key-0`, value: `value-${value1}` }
        const value2 = secureRandom()
        const message2 = { key: `key-0`, value: `value-${value2}` }
        const value3 = secureRandom()
        const message3 = { key: `key-0`, value: `value-${value3}` }

        await producer.send({ acks: 1, topic: topicName, messages: [message1, message2, message3] })
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 0, offset: 0 })
        consumer.seek({ topic: topicName, partition: 0, offset: 1 })
        consumer.seek({ topic: topicName, partition: 0, offset: 2 })

        await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '2' }),
            }),
          ])
        )
      })
    })
  })
})
