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
  waitForConsumerToJoinGroup,
  testIfKafkaAtLeast_0_11,
} = require('testHelpers')

describe('Consumer > Instrumentation Events', () => {
  let topicName, groupId, cluster, producer, consumer, consumer2, message, emitter

  const createTestConsumer = (opts = {}) =>
    createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 500,
      maxBytesPerPartition: 180,
      rebalanceTimeout: 1000,
      instrumentationEmitter: emitter,
      ...opts,
    })

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    emitter = new InstrumentationEventEmitter()
    cluster = createCluster({ instrumentationEmitter: emitter, metadataMaxAge: 50 })
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    consumer2 && (await consumer2.disconnect())
    producer && (await producer.disconnect())
  })

  test('on throws an error when provided with an invalid event name', () => {
    consumer = createTestConsumer()
    expect(() => consumer.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      /Event name should be one of consumer.events./
    )
  })

  it('emits heartbeat', async () => {
    const onHeartbeat = jest.fn()
    let heartbeats = 0

    consumer = createTestConsumer({ heartbeatInterval: 0 })
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

    consumer = createTestConsumer()
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

    consumer = createTestConsumer()
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

    consumer = createTestConsumer()
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
        nodeId: expect.any(String),
      },
    })
  })

  it('emits fetch start', async () => {
    const onFetchStart = jest.fn()
    let fetch = 0

    consumer = createTestConsumer()
    consumer.on(consumer.events.FETCH_START, async event => {
      onFetchStart(event)
      fetch++
    })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    await consumer.run({ eachMessage: () => true })

    await waitFor(() => fetch > 0)
    expect(onFetchStart).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.fetch_start',
      payload: {
        nodeId: expect.any(String),
      },
    })
  })

  it('emits start batch process', async () => {
    const onStartBatchProcess = jest.fn()
    let startBatchProcess = 0

    consumer = createTestConsumer()
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
        offsetLagLow: expect.any(String),
        batchSize: 1,
        firstOffset: expect.any(String),
        lastOffset: expect.any(String),
      },
    })
  })

  it('emits end batch process', async () => {
    const onEndBatchProcess = jest.fn()
    let endBatchProcess = 0

    consumer = createTestConsumer()
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
        offsetLagLow: expect.any(String),
        batchSize: 1,
        firstOffset: expect.any(String),
        lastOffset: expect.any(String),
        duration: expect.any(Number),
      },
    })
  })

  testIfKafkaAtLeast_0_11(
    'emits start and end batch process when reading empty control batches',
    async () => {
      const startBatchProcessSpy = jest.fn()
      const endBatchProcessSpy = jest.fn()

      consumer = createTestConsumer()
      consumer.on(consumer.events.START_BATCH_PROCESS, startBatchProcessSpy)
      consumer.on(consumer.events.END_BATCH_PROCESS, endBatchProcessSpy)

      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      producer = createProducer({
        cluster,
        createPartitioner: createModPartitioner,
        logger: newLogger(),
        transactionalId: `test-producer-${secureRandom()}`,
        maxInFlightRequests: 1,
        idempotent: true,
      })

      await producer.connect()
      const transaction = await producer.transaction()

      await transaction.send({
        topic: topicName,
        acks: -1,
        messages: [
          {
            key: 'test',
            value: 'test',
          },
        ],
      })
      await transaction.abort()

      await waitFor(
        () => startBatchProcessSpy.mock.calls.length > 0 && endBatchProcessSpy.mock.calls.length > 0
      )

      expect(startBatchProcessSpy).toHaveBeenCalledWith({
        id: expect.any(Number),
        timestamp: expect.any(Number),
        type: 'consumer.start_batch_process',
        payload: {
          topic: topicName,
          partition: 0,
          highWatermark: '2',
          offsetLag: expect.any(String),
          offsetLagLow: expect.any(String),
          batchSize: 0,
          firstOffset: '0',
          lastOffset: '1',
        },
      })
      expect(startBatchProcessSpy).toHaveBeenCalledTimes(1)

      expect(endBatchProcessSpy).toHaveBeenCalledWith({
        id: expect.any(Number),
        timestamp: expect.any(Number),
        type: 'consumer.end_batch_process',
        payload: {
          topic: topicName,
          partition: 0,
          highWatermark: '2',
          offsetLag: expect.any(String),
          offsetLagLow: expect.any(String),
          batchSize: 0,
          firstOffset: '0',
          lastOffset: '1',
          duration: expect.any(Number),
        },
      })
      expect(endBatchProcessSpy).toHaveBeenCalledTimes(1)
    }
  )

  it('emits connection events', async () => {
    const connectListener = jest.fn().mockName('connect')
    const disconnectListener = jest.fn().mockName('disconnect')
    const stopListener = jest.fn().mockName('stop')

    consumer = createTestConsumer()
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
    const crashListener = jest.fn()
    const error = new Error('💣')
    const eachMessage = jest.fn().mockImplementationOnce(() => {
      throw error
    })

    consumer = createTestConsumer({ retry: { retries: 0 } })
    consumer.on(consumer.events.CRASH, crashListener)

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage })

    await producer.connect()
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)

    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId, restart: true },
    })
  })

  it('emits crash events with restart=false', async () => {
    const crashListener = jest.fn()
    const error = new Error('💣💥')
    const eachMessage = jest.fn().mockImplementationOnce(() => {
      throw error
    })

    consumer = createTestConsumer({ retry: { retries: 0, restartOnFailure: async () => false } })
    consumer.on(consumer.events.CRASH, crashListener)

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage })

    await producer.connect()
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)

    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId, restart: false },
    })
  })

  it('emits rebalancing', async () => {
    const onRebalancing = jest.fn()

    const groupId = `consumer-group-id-${secureRandom()}`

    consumer = createTestConsumer({
      groupId,
      cluster: createCluster({
        instrumentationEmitter: new InstrumentationEventEmitter(),
        metadataMaxAge: 50,
      }),
    })

    consumer2 = createTestConsumer({
      groupId,
      cluster: createCluster({
        instrumentationEmitter: new InstrumentationEventEmitter(),
        metadataMaxAge: 50,
      }),
    })

    let memberId
    consumer.on(consumer.events.GROUP_JOIN, async event => {
      memberId = memberId || event.payload.memberId
    })

    consumer.on(consumer.events.REBALANCING, async event => {
      onRebalancing(event)
    })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    consumer.run({ eachMessage: () => true })

    await waitForConsumerToJoinGroup(consumer, { label: 'consumer1' })

    await consumer2.connect()
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    consumer2.run({ eachMessage: () => true })

    await waitForConsumerToJoinGroup(consumer2, { label: 'consumer2' })

    expect(onRebalancing).toBeCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.rebalancing',
      payload: {
        groupId: groupId,
        memberId: memberId,
      },
    })
  })

  it('emits request events', async () => {
    const requestListener = jest.fn().mockName('request')

    consumer = createTestConsumer()
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
    cluster = createCluster({
      instrumentationEmitter: emitter,
      requestTimeout: 1,
      enforceRequestTimeout: true,
    })
    const requestListener = jest.fn().mockName('request_timeout')

    consumer = createTestConsumer({ cluster })
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
        apiKey: expect.any(Number),
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

  /**
   * This test is too flaky, we need to think about a better way of testing this.
   * Skipping until we have a better plan
   */
  it.skip('emits request queue size events', async () => {
    const cluster = createCluster({
      instrumentationEmitter: emitter,
      maxInFlightRequests: 1,
    })

    const requestListener = jest.fn().mockName('request_queue_size')

    consumer = createTestConsumer({ cluster })
    consumer.on(consumer.events.REQUEST_QUEUE_SIZE, requestListener)

    consumer2 = createTestConsumer({ cluster })
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

  it('emits received unsubscribed topics events', async () => {
    const topicNames = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`]
    const otherTopic = `test-topic-${secureRandom()}`
    const groupId = `consumer-group-id-${secureRandom()}`

    for (const topicName of [...topicNames, otherTopic]) {
      await createTopic({ topic: topicName, partitions: 2 })
    }

    // First consumer subscribes to topicNames
    consumer = createTestConsumer({
      groupId,
      cluster: createCluster({
        instrumentationEmitter: new InstrumentationEventEmitter(),
        metadataMaxAge: 50,
      }),
    })

    await consumer.connect()
    await Promise.all(
      topicNames.map(topicName => consumer.subscribe({ topic: topicName, fromBeginning: true }))
    )

    consumer.run({ eachMessage: () => {} })
    await waitForConsumerToJoinGroup(consumer, { label: 'consumer1' })

    // Second consumer re-uses group id but only subscribes to one of the topics
    consumer2 = createTestConsumer({
      groupId,
      cluster: createCluster({
        instrumentationEmitter: new InstrumentationEventEmitter(),
        metadataMaxAge: 50,
      }),
    })

    const onReceivedUnsubscribedTopics = jest.fn()
    let receivedUnsubscribedTopics = 0
    consumer2.on(consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS, async event => {
      onReceivedUnsubscribedTopics(event)
      receivedUnsubscribedTopics++
    })

    await consumer2.connect()
    await Promise.all(
      [topicNames[1], otherTopic].map(topicName =>
        consumer2.subscribe({ topic: topicName, fromBeginning: true })
      )
    )

    consumer2.run({ eachMessage: () => {} })
    await waitForConsumerToJoinGroup(consumer2, { label: 'consumer2' })

    // Wait for rebalance to finish
    await waitFor(async () => {
      const { state, members } = await consumer.describeGroup()
      return state === 'Stable' && members.length === 2
    })

    await waitFor(() => receivedUnsubscribedTopics > 0)

    expect(onReceivedUnsubscribedTopics).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.received_unsubscribed_topics',
      payload: {
        groupId,
        generationId: expect.any(Number),
        memberId: expect.any(String),
        assignedTopics: topicNames,
        topicsSubscribed: [topicNames[1], otherTopic],
        topicsNotSubscribed: [topicNames[0]],
      },
    })
  })
})
