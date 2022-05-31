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
    it('throws an error if any of the topics is invalid', async () => {
      await expect(consumer.commitOffsets([{ topic: null }])).rejects.toThrow(
        KafkaJSNonRetriableError,
        'Invalid topic null'
      )
    })

    it('throws an error if any of the partitions is not a number', async () => {
      await expect(
        consumer.commitOffsets([{ topic: topicName, partition: 'ABC' }])
      ).rejects.toThrow(
        KafkaJSNonRetriableError,
        'Invalid partition, expected a number received ABC'
      )
    })

    it('throws an error if any of the offsets is not a number', async () => {
      await expect(
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: 'ABC' }])
      ).rejects.toThrow(KafkaJSNonRetriableError, 'Invalid offset, expected a long received ABC')
    })

    it('throws an error if any of the offsets is not an absolute offset', async () => {
      await expect(
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '-1' }])
      ).rejects.toThrow(KafkaJSNonRetriableError, 'Offset must not be a negative number')
    })

    it('throws an error if called before consumer run', async () => {
      await expect(
        consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '1' }])
      ).rejects.toThrow(
        KafkaJSNonRetriableError,
        'Consumer group was not initialized, consumer#run must be called first'
      )
    })

    it('updates the partition committed offset to the given offset', async () => {
      await consumer.connect()
      await producer.connect()

      const messages = Array(3)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await producer.send({ acks: 1, topic: topicName, messages })

      const offsetsConsumed = []

      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      consumer.run({
        autoCommit: false,
        eachMessage: async event => {
          offsetsConsumed.push(event.message.offset)
          if (offsetsConsumed.length === 1) {
            await consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '1' }])
          }
        },
      })

      await waitForConsumerToJoinGroup(consumer)
      await expect(waitForMessages(offsetsConsumed, { number: 3 })).resolves.toEqual([
        '0',
        '1',
        '2',
      ])

      await waitForMessages(offsetsConsumed, { number: 3 })

      expect(cluster.committedOffsets({ groupId })[topicName][0].toString()).toEqual('1')
    })
  })
})
