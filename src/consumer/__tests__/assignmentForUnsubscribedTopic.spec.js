const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicNames, groupId, consumer1, consumer2

  beforeEach(async () => {
    topicNames = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]
    groupId = `consumer-group-id-${secureRandom()}`

    await Promise.all(topicNames.map(topicName => createTopic({ topic: topicName })))

    consumer1 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
  })

  it('handles receiving assignments for unsubscribed topics', async () => {
    await consumer1.connect()
    await Promise.all(
      topicNames.map(topicName => consumer1.subscribe({ topic: topicName, fromBeginning: true }))
    )

    consumer1.run({ eachMessage: () => {} })
    await waitForConsumerToJoinGroup(consumer1)

    // Second consumer re-uses group id but only subscribes to one of the topics
    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
    await consumer2.subscribe({ topic: topicNames[0], fromBeginning: true })

    consumer2.run({ eachMessage: () => {} })
    await waitForConsumerToJoinGroup(consumer2)
  })
})
