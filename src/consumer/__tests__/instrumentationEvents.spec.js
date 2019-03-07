const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const createProducer = require('../../producer')
const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
} = require('testHelpers')

describe('Consumer > Instrumentation Events', () => {
  let topicName, groupId, cluster, producer, consumer, message, emitter

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    emitter = new InstrumentationEventEmitter()
    cluster = createCluster({ instrumentationEmitter: emitter })
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
      instrumentationEmitter: emitter,
    })

    message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  test('on throws an error when provided with an invalid event name', () => {
    expect(() => consumer.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      /Event name should be one of consumer.events./
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
        groupProtocol: expect.any(String),
        memberId: expect.any(String),
        memberAssignment: { [topicName]: [0] },
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

  it('emits crash events', async () => {
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      retry: {
        retries: 0,
      },
    })
    const crashListener = jest.fn()
    consumer.on(consumer.events.CRASH, crashListener)
    const error = new Error('💣')

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {
        throw error
      },
    })

    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId },
    })
  })

  it('emits request events', async () => {
    const requestListener = jest.fn().mockName('request')
    consumer.on(consumer.events.REQUEST, requestListener)

    await consumer.connect()
    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.network.request',
      payload: {
        apiKey: 18,
        apiName: 'ApiVersions',
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        duration: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
        size: expect.any(Number),
      },
    })
  })

  it('emits request timeout events', async () => {
    cluster = createCluster({ instrumentationEmitter: emitter, requestTimeout: 1 })
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request_timeout')
    consumer.on(consumer.events.REQUEST_TIMEOUT, requestListener)

    await consumer
      .connect()
      .then(() => consumer.run({ eachMessage: () => true }))
      .catch(e => e)

    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.network.request_timeout',
      payload: {
        apiKey: 18,
        apiName: expect.any(String),
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
      },
    })
  })

  it('emits request queue size events', async () => {
    const cluster = createCluster({
      instrumentationEmitter: emitter,
      maxInFlightRequests: 1,
    })

    consumer = createConsumer({
      groupId,
      cluster,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      instrumentationEmitter: emitter,
    })

    const consumer2 = createConsumer({
      groupId,
      cluster,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request_queue_size')
    consumer.on(consumer.events.REQUEST_QUEUE_SIZE, requestListener)
    consumer2.on(consumer2.events.REQUEST_QUEUE_SIZE, requestListener)

    await Promise.all([
      consumer
        .connect()
        .then(() => consumer.run({ eachMessage: () => true }))
        .catch(e => e),
      consumer2
        .connect()
        .then(() => consumer.run({ eachMessage: () => true }))
        .catch(e => e),
    ])

    // add more concurrent requests to make we increate the requests
    // on the queue
    await Promise.all([
      consumer.describeGroup(),
      consumer.describeGroup(),
      consumer.describeGroup(),
      consumer.describeGroup(),
      consumer2.describeGroup(),
      consumer2.describeGroup(),
      consumer2.describeGroup(),
      consumer2.describeGroup(),
    ])

    await consumer2.disconnect()

    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.network.request_queue_size',
      payload: {
        broker: expect.any(String),
        clientId: expect.any(String),
        queueSize: expect.any(Number),
      },
    })
  })
})
