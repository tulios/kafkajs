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

  describe('when subscribe', () => {
    it('throws an error if the topic is invalid', async () => {
      await expect(consumer.subscribe({ topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    it('throws an error if the topic is not a String or RegExp', async () => {
      await expect(consumer.subscribe({ topic: 1 })).rejects.toHaveProperty(
        'message',
        'Invalid topic 1 (number), the topic name has to be a String or a RegExp'
      )
    })
  })

  describe('with regex', () => {
    it('subscribes to all matching topics', async () => {
      const testScope = secureRandom()
      const topicUS = `pattern-${testScope}-us-${secureRandom()}`
      const topicSE = `pattern-${testScope}-se-${secureRandom()}`
      const topicUK = `pattern-${testScope}-uk-${secureRandom()}`
      const topicBR = `pattern-${testScope}-br-${secureRandom()}`

      await createTopic({ topic: topicUS })
      await createTopic({ topic: topicSE })
      await createTopic({ topic: topicUK })
      await createTopic({ topic: topicBR })

      const messagesConsumed = []
      await consumer.connect()
      await consumer.subscribe({
        topic: new RegExp(`pattern-${testScope}-(se|br)-.*`, 'i'),
        fromBeginning: true,
      })

      consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
      await waitForConsumerToJoinGroup(consumer)

      await producer.connect()
      await producer.sendBatch({
        acks: 1,
        topicMessages: [
          { topic: topicUS, messages: [{ key: `key-us`, value: `value-us` }] },
          { topic: topicUK, messages: [{ key: `key-uk`, value: `value-uk` }] },
          { topic: topicSE, messages: [{ key: `key-se`, value: `value-se` }] },
          { topic: topicBR, messages: [{ key: `key-br`, value: `value-br` }] },
        ],
      })

      await waitForMessages(messagesConsumed, { number: 2 })
      expect(messagesConsumed.map(m => m.message.value.toString()).sort()).toEqual([
        'value-br',
        'value-se',
      ])
    })
  })
})
