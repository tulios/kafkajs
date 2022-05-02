const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
  createModPartitioner,
  waitForNextEvent,
} = require('testHelpers')
const createConsumer = require('../index')
const createProducer = require('../../producer')
const sleep = require('../../utils/sleep')
const waitFor = require('../../utils/waitFor')
const uniq = require('../../utils/uniq')

describe('Consumer', () => {
  let topicName, groupId, consumer, consumer2, producer

  describe('#run', () => {
    beforeEach(async () => {
      topicName = `test-topic-${secureRandom()}`
      groupId = `consumer-group-id-${secureRandom()}`

      await createTopic({ topic: topicName })
      consumer = createConsumer({
        cluster: createCluster({ metadataMaxAge: 50 }),
        groupId,
        heartbeatInterval: 100,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })
      consumer2 = createConsumer({
        cluster: createCluster({ metadataMaxAge: 50 }),
        groupId,
        heartbeatInterval: 100,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })
      producer = createProducer({
        cluster: createCluster({ metadataMaxAge: 50 }),
        logger: newLogger(),
        createPartitioner: createModPartitioner,
      })
    })

    afterEach(async () => {
      consumer && (await consumer.disconnect())
      consumer2 && (await consumer2.disconnect())
      producer && (await producer.disconnect())
    })

    describe('when the consumer is already running', () => {
      it('ignores the call', async () => {
        await consumer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })
        const eachMessage = jest.fn()

        Promise.all([
          consumer.run({ eachMessage }),
          consumer.run({ eachMessage }),
          consumer.run({ eachMessage }),
        ])

        // Since the consumer gets overridden, it will fail to join the group
        // as three other consumers will also try to join. This case is hard to write a test
        // since we can only assert the symptoms of the problem, but we can't assert that
        // we don't initialize the consumer.
        await waitForConsumerToJoinGroup(consumer)
      })
    })

    it('should not read duplicate messages during rebalance', async () => {
      topicName = `test-topic-${secureRandom()}`
      await createTopic({ topic: topicName, partitions: 2 })

      await producer.connect()
      await producer.send({
        topic: topicName,
        messages: Array(10)
          .fill()
          .map((_, index) => {
            return { key: `key-${index % 2}`, value: `value-${index}` }
          }),
      })
      await producer.disconnect()

      const consumerMessages = []
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      await consumer.run({
        eachMessage: async event => {
          await sleep(100)
          consumerMessages.push(`${event.partition} ${event.message.value.toString()}`)
        },
      })

      await waitForNextEvent(consumer, consumer.events.START_BATCH_PROCESS)

      const consumer2Messages = []
      await consumer2.connect()
      await consumer2.subscribe({ topic: topicName, fromBeginning: true })
      await consumer2.run({
        eachMessage: async event => {
          await sleep(100)
          consumer2Messages.push(`${event.partition} ${event.message.value.toString()}`)
        },
      })

      await waitFor(() => consumerMessages.length + consumer2Messages.length >= 10)
      await sleep(500)

      expect([...consumerMessages, ...consumer2Messages]).toHaveLength(10)
      expect(uniq([...consumerMessages, ...consumer2Messages])).toHaveLength(10)
    })
  })
})
