const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')
const createConsumer = require('../index')

describe('Consumer', () => {
  let topicName, groupId, consumer

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
    })

    afterEach(async () => {
      consumer && (await consumer.disconnect())
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
  })
})
