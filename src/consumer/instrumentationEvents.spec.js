const createProducer = require('../producer')
const createConsumer = require('./index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
} = require('testHelpers')

describe('Consumer > Instrumentation Events', () => {
  let topicName, groupId, cluster, producer, consumer, message

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      heartbeatInterval: 100,
    })

    message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }
  })

  afterEach(async () => {
    await consumer.disconnect()
    await producer.disconnect()
  })

  it('emits heartbeat', async () => {
    const onHeartbeat = jest.fn()
    let heartbeats = 0
    consumer.on(consumer.events.HEARTBEAT, async event => {
      onHeartbeat(event)
      heartbeats++
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    consumer.run({ eachMessage: () => true })
    await producer.send({ topic: topicName, messages: [message] })

    await waitFor(() => heartbeats > 0)
    expect(onHeartbeat).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.heartbeat',
      payload: {
        groupId,
        memberId: expect.any(String),
        groupGenerationId: expect.any(Number),
      },
    })
  })

  it.only('emits commit offsets', async () => {
    const onCommitOffsets = jest.fn()
    let commitOffsets = 0
    consumer.on(consumer.events.COMMIT_OFFSETS, async event => {
      console.log('Committed offsets', event)
      onCommitOffsets(event)
      commitOffsets++
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    console.log('Starting run')
    consumer.run({ eachMessage: () => true })
    await producer.send({ topic: topicName, messages: [message] })
    console.log('Has sent')

    await waitFor(() => commitOffsets > 0)
    expect(onCommitOffsets).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.commit_offsets',
      payload: {
        groupId,
        memberId: expect.any(String),
        groupGenerationId: expect.any(Number),
        topics: [
          {
            topic: topicName,
            partitions: [
              {
                offset: '1',
                partition: '0',
              },
            ],
          },
        ],
      },
    })
  })
})
