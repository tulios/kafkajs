const createProducer = require('../../producer')
const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

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
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
    })

    message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }
  })

  afterEach(async () => {
    await consumer.disconnect()
    await producer.disconnect()
  })

  test('on throws an error when provided with an invalid event name', () => {
    expect(() => consumer.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      KafkaJSNonRetriableError,
      /Event name should be one of/
    )
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

    await consumer.run({ eachMessage: () => true })
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

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

  it('emits commit offsets', async () => {
    const onCommitOffsets = jest.fn()
    let commitOffsets = 0
    consumer.on(consumer.events.COMMIT_OFFSETS, async event => {
      onCommitOffsets(event)
      commitOffsets++
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage: () => true })
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

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

  it('emits group join', async () => {
    const onGroupJoin = jest.fn()
    let groupJoin = 0
    consumer.on(consumer.events.GROUP_JOIN, async event => {
      onGroupJoin(event)
      groupJoin++
    })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    await consumer.run({ eachMessage: () => true })

    await waitFor(() => groupJoin > 0)
    expect(onGroupJoin).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.group_join',
      payload: {
        duration: expect.any(Number),
        groupId: expect.any(String),
        isLeader: true,
        leaderId: expect.any(String),
        memberId: expect.any(String),
      },
    })
  })

  it('emits fetch', async () => {
    const onFetch = jest.fn()
    let fetch = 0
    consumer.on(consumer.events.FETCH, async event => {
      onFetch(event)
      fetch++
    })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    await consumer.run({ eachMessage: () => true })

    await waitFor(() => fetch > 0)
    expect(onFetch).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.fetch',
      payload: {
        numberOfBatches: expect.any(Number),
        duration: expect.any(Number),
      },
    })
  })

  it('emits start batch process', async () => {
    const onStartBatchProcess = jest.fn()
    let startBatchProcess = 0
    consumer.on(consumer.events.START_BATCH_PROCESS, async event => {
      onStartBatchProcess(event)
      startBatchProcess++
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage: () => true })
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => startBatchProcess > 0)
    expect(onStartBatchProcess).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.start_batch_process',
      payload: {
        topic: topicName,
        partition: 0,
        highWatermark: expect.any(String),
        offsetLag: expect.any(String),
        batchSize: 1,
        firstOffset: expect.any(String),
        lastOffset: expect.any(String),
      },
    })
  })

  it('emits end batch process', async () => {
    const onEndBatchProcess = jest.fn()
    let endBatchProcess = 0
    consumer.on(consumer.events.END_BATCH_PROCESS, async event => {
      onEndBatchProcess(event)
      endBatchProcess++
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage: () => true })
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => endBatchProcess > 0)
    expect(onEndBatchProcess).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.end_batch_process',
      payload: {
        topic: topicName,
        partition: 0,
        highWatermark: expect.any(String),
        offsetLag: expect.any(String),
        batchSize: 1,
        firstOffset: expect.any(String),
        lastOffset: expect.any(String),
        duration: expect.any(Number),
      },
    })
  })

  it('emits connection events', async () => {
    const connectListener = jest.fn().mockName('connect')
    const disconnectListener = jest.fn().mockName('disconnect')
    const stopListener = jest.fn().mockName('stop')
    consumer.on(consumer.events.CONNECT, connectListener)
    consumer.on(consumer.events.DISCONNECT, disconnectListener)
    consumer.on(consumer.events.STOP, stopListener)

    await consumer.connect()
    expect(connectListener).toHaveBeenCalled()

    await consumer.run()

    await consumer.disconnect()
    expect(stopListener).toHaveBeenCalled()
    expect(disconnectListener).toHaveBeenCalled()
  })
})
