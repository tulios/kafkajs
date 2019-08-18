const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')
const sleep = require('../../utils/sleep')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitFor,
  waitForMessages,
  waitForNextEvent,
  testIfKafka_0_11,
  waitForConsumerToJoinGroup,
  generateMessages,
} = require('testHelpers')

describe('Consumer > Heartbeat', () => {
  let topicName, groupId, cluster, consumer
  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    // await createTopic({ topic: topicName })

    cluster = createCluster()

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      heartbeatInterval: 0,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    await consumer.disconnect()
  })

  it('throws an error if the consumer group is not initialized', async () => {
    await expect(consumer.heartbeat()).rejects.toEqual(
      new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    )
  })

  it('heartbeats when called', async () => {
    const onHeartbeat = jest.fn()
    consumer.on(consumer.events.HEARTBEAT, onHeartbeat)

    await consumer.connect()
    consumer.run({
      eachMessage: async () => {
        await sleep(10000)
      },
    })
    await waitForConsumerToJoinGroup(consumer)
    expect(onHeartbeat).not.toHaveBeenCalled()

    await consumer.heartbeat()
    expect(onHeartbeat).toHaveBeenCalled()
  })
})
