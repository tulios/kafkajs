const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
  flakyTest,
} = require('testHelpers')

describe('Consumer', () => {
  let topicNames, groupId, consumer1, consumer2

  beforeEach(async () => {
    topicNames = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]
    groupId = `consumer-group-id-${secureRandom()}`

    await Promise.all(topicNames.map(topicName => createTopic({ topic: topicName, partitions: 2 })))

    consumer1 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 1000,
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
    await waitForConsumerToJoinGroup(consumer1, { label: 'consumer1' })

    // Second consumer re-uses group id but only subscribes to one of the topics
    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1000,
      rebalanceTimeout: 1000,
      logger: newLogger(),
    })

    await consumer2.connect()
    await consumer2.subscribe({ topic: topicNames[0], fromBeginning: true })

    consumer2.run({ eachMessage: () => {} })
    const event = await waitForConsumerToJoinGroup(consumer2, { label: 'consumer2' })

    // verify that the assignment does not contain the unsubscribed topic
    expect(event.payload.memberAssignment[topicNames[1]]).toBeUndefined()
  })

  flakyTest('starts consuming from new topics after already having assignments', async () => {
    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 1000,
      logger: newLogger(),
    })

    // Both consumers receive assignments for one topic
    let assignments = await Promise.all(
      [consumer1, consumer2].map(async consumer => {
        await consumer.connect()
        await consumer.subscribe({ topic: topicNames[0] })
        consumer.run({ eachMessage: () => {} })
        return waitForConsumerToJoinGroup(consumer)
      })
    )

    assignments.forEach(assignment =>
      expect(Object.keys(assignment.payload.memberAssignment)).toEqual([topicNames[0]])
    )

    // One consumer is replaced with a new one, subscribing to the old topic as well as a new one
    await consumer1.disconnect()
    consumer1 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 1000,
      logger: newLogger(),
    })

    await consumer1.connect()
    await Promise.all(topicNames.map(topic => consumer1.subscribe({ topic })))

    // Second consumer is also replaced, subscribing to both topics
    await consumer2.disconnect()
    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 1000,
      logger: newLogger(),
    })

    await consumer2.connect()
    await Promise.all(topicNames.map(topic => consumer2.subscribe({ topic })))

    consumer1.run({ eachMessage: () => {} })
    consumer2.run({ eachMessage: () => {} })

    // Both consumers are assigned to both topics
    assignments = await Promise.all(
      [consumer1, consumer2].map(consumer => waitForConsumerToJoinGroup(consumer))
    )

    assignments.forEach(assignment =>
      expect(Object.keys(assignment.payload.memberAssignment)).toEqual(topicNames)
    )
  })
})
