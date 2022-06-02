const createAdmin = require('../../admin')
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
          'Consumer group was not initialized, consumer#run must be called first'
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

        await waitForConsumerToJoinGroup(consumer)
        await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
          expect.objectContaining({
            topic: topicName,
            partition: 0,
            message: expect.objectContaining({ offset: '0' }),
          }),
        ])
      })

      describe('When "autoCommit" is false', () => {
        let admin

        beforeEach(() => {
          admin = createAdmin({ logger: newLogger(), cluster })
        })

        afterEach(async () => {
          admin && (await admin.disconnect())
        })

        it('should not commit the offset', async () => {
          await Promise.all([consumer, producer, admin].map(client => client.connect()))

          await producer.send({
            acks: 1,
            topic: topicName,
            messages: [1, 2, 3].map(n => ({ key: `key-${n}`, value: `value-${n}` })),
          })
          await consumer.subscribe({ topic: topicName, fromBeginning: true })

          let messagesConsumed = []
          consumer.run({
            autoCommit: false,
            eachMessage: async event => messagesConsumed.push(event),
          })
          consumer.seek({ topic: topicName, partition: 0, offset: 2 })

          await waitForConsumerToJoinGroup(consumer)
          await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '2' }),
            }),
          ])

          await expect(admin.fetchOffsets({ groupId, topics: [topicName] })).resolves.toEqual([
            {
              topic: topicName,
              partitions: expect.arrayContaining([
                expect.objectContaining({
                  partition: 0,
                  offset: '-1',
                }),
              ]),
            },
          ])

          messagesConsumed = []
          consumer.seek({ topic: topicName, partition: 0, offset: 1 })

          await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '1' }),
            }),
            expect.objectContaining({
              topic: topicName,
              partition: 0,
              message: expect.objectContaining({ offset: '2' }),
            }),
          ])
        })
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
        // must be called after run because the ConsumerGroup must be initialized
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 1, offset: 1 })

        await waitForConsumerToJoinGroup(consumer)
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
        // must be called after run because the ConsumerGroup must be initialized
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        consumer.seek({ topic: topicName, partition: 0, offset: 2 })
        consumer.seek({ topic: topicName, partition: 1, offset: 1 })

        await waitForConsumerToJoinGroup(consumer)
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

        await waitForConsumerToJoinGroup(consumer)
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
