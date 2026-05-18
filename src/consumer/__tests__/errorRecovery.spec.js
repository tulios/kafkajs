const createProducer = require('../../producer')
const createConsumer = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSNonRetriableError,
  KafkaJSOffsetOutOfRange,
} = require('../../errors')

const sleep = require('../../utils/sleep')
const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
  waitForMessages,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer, consumer2

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
    })

    await producer.connect()
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    consumer2 && (await consumer2.disconnect())
    producer && (await producer.disconnect())
  })

  it('recovers from offset out of range', async () => {
    await consumer.connect()
    await producer.connect()

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const { generationId, memberId } = await coordinator.joinGroup({
      groupId,
      sessionTimeout: 6000,
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
        },
      ],
    })

    const memberAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { [topicName]: [0] },
    })

    const groupAssignment = [{ memberId, memberAssignment }]
    await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment,
    })

    const topics = [
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: '11' }],
      },
    ]

    await coordinator.offsetCommit({
      groupId,
      groupGenerationId: generationId,
      memberId,
      topics,
    })

    await coordinator.leaveGroup({ groupId, memberId })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message1] })

    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    await expect(waitForMessages(messagesConsumed)).resolves.toEqual([
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      }),
    ])
  })

  // ---------------------------------------------------------------------------
  // SKIPPED — executable feature request. Un-skip once the option below
  // exists.
  //
  // Context
  //   KafkaJS's current handling of OFFSET_OUT_OF_RANGE is documented and
  //   intentional: `consumerGroup.recoverFromOffsetOutOfRange` calls
  //   `offsetManager.setDefaultOffset()` and resumes from EARLIEST or LATEST
  //   based on the `fromBeginning` subscribe flag. That is the existing
  //   contract, and changing it by default would be a breaking change.
  //
  // Gap
  //   There is no way to opt out of the automatic reset. The Java client and
  //   librdkafka both expose `auto.offset.reset=none` (librdkafka calls it
  //   `error`) for exactly this case: when the committed position is lost
  //   (topic recreated, retention expired, __consumer_offsets partition lost,
  //   manual reset to a bad value) the consumer fails fast so the operator
  //   can decide what to do.
  //
  //   Without this option, the same scenario in KafkaJS resolves to either
  //     - silent reprocessing of the whole topic (`fromBeginning: true`), or
  //     - silent skipping of every record between the lost offset and the
  //       current log end (`fromBeginning: false`),
  //   indistinguishable from normal operation in logs.
  //
  // What this test asserts (the proposed contract)
  //   When the consumer is created with `autoOffsetReset: 'none'`, an
  //   out-of-range committed offset surfaces as a `KafkaJSOffsetOutOfRange`
  //   on the existing `consumer.crash` event, and no message is delivered
  //   to `eachMessage`. The default (option omitted) is unchanged.
  //
  // Why this test is skipped today
  //   `autoOffsetReset` is not a recognised option, so the consumer takes
  //   the existing reset path and starts delivering messages. The
  //   `waitFor(crashListener)` then times out — i.e. the test is skipped
  //   because the feature it documents does not yet exist, not because of
  //   a defect in current code.
  //
  // Suggested implementation (small, additive, backwards compatible)
  //   1. Add `autoOffsetReset?: 'earliest' | 'latest' | 'none'` to
  //      `ConsumerConfig` (types/index.d.ts) and thread it through
  //      `src/consumer/index.js` into `consumerGroup`.
  //   2. In `consumerGroup.recoverFromOffsetOutOfRange`, when
  //      `autoOffsetReset === 'none'`, re-throw the `KafkaJSOffsetOutOfRange`
  //      instead of calling `offsetManager.setDefaultOffset()`. The existing
  //      crash machinery handles the rest (this test verifies that).
  //   3. Omitted / any other value → existing behaviour, unchanged.
  //
  // Refs
  //   https://github.com/tulios/kafkajs/issues/580
  //   https://github.com/tulios/kafkajs/issues/1119
  //   Java client: `auto.offset.reset=none`
  //   librdkafka:  `auto.offset.reset=error`
  // ---------------------------------------------------------------------------
  it.skip('crashes with KafkaJSOffsetOutOfRange instead of silently resetting when autoOffsetReset is "none"', async () => {
    // Arrange: a consumer that opts out of automatic offset reset.
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      autoOffsetReset: 'none',
      retry: { retries: 0 },
    })

    await consumer.connect()

    // Arrange: commit offset 11 for a partition whose log will only contain
    // offset 0. This is the canonical out-of-range scenario.
    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const { generationId, memberId } = await coordinator.joinGroup({
      groupId,
      sessionTimeout: 6000,
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
        },
      ],
    })

    const memberAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { [topicName]: [0] },
    })

    await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment: [{ memberId, memberAssignment }],
    })

    await coordinator.offsetCommit({
      groupId,
      groupGenerationId: generationId,
      memberId,
      topics: [
        {
          topic: topicName,
          partitions: [{ partition: 0, offset: '11' }],
        },
      ],
    })

    await coordinator.leaveGroup({ groupId, memberId })

    // Arrange: log end offset = 1, committed offset = 11.
    // Fetching from 11 will return OFFSET_OUT_OF_RANGE.
    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message1] })

    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const crashListener = jest.fn()
    consumer.on(consumer.events.CRASH, crashListener)

    const eachMessage = jest.fn()

    // Act: start consuming. The broker will respond with OFFSET_OUT_OF_RANGE
    // for partition 0 on the first fetch.
    consumer.run({ eachMessage })

    // Assert: with autoOffsetReset='none' the consumer must surface the error
    // as a crash. Today this option does not exist, so the consumer falls
    // back to the default reset path and this wait times out — the timeout
    // is what makes the gap observable.
    await waitFor(() => crashListener.mock.calls.length > 0, {
      maxWait: 8000,
      ignoreTimeout: false,
      timeoutMessage:
        'Consumer did not crash on OFFSET_OUT_OF_RANGE — it silently reset offsets (no autoOffsetReset="none" opt-out available)',
    })

    // Assert: the crash payload is a KafkaJSOffsetOutOfRange, not a generic
    // error, so application code can pattern-match on it.
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: {
        groupId,
        error: expect.any(KafkaJSOffsetOutOfRange),
        restart: expect.any(Boolean),
      },
    })

    // Assert: no message was delivered to the application. A silent reset
    // would have delivered message1 here — that is the data-integrity hazard.
    expect(eachMessage).not.toHaveBeenCalled()
  })

  it('recovers from crashes due to retriable errors', async () => {
    const groupId = `consumer-group-id-${secureRandom()}`
    consumer2 = createConsumer({
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
    consumer2.on(consumer.events.CRASH, crashListener)

    const error = new KafkaJSError(new Error('💣'), { retriable: true })

    await consumer2.connect()
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const original = coordinator.joinGroup
    coordinator.joinGroup = async () => {
      coordinator.joinGroup = original
      throw error
    }

    const eachMessage = jest.fn()
    await consumer2.run({ eachMessage })

    const key = secureRandom()
    const message = { key: `key-${key}`, value: `value-${key}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId, restart: true },
    })

    await expect(waitFor(() => eachMessage.mock.calls.length)).resolves.toBe(1)
  })

  it('does not recover from crashes due to not retriable errors', async () => {
    const groupId = `consumer-group-id-${secureRandom()}`
    consumer2 = createConsumer({
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
    consumer2.on(consumer.events.CRASH, crashListener)

    const error = new KafkaJSError(new Error('💣'), { retriable: false })

    await consumer2.connect()
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const original = coordinator.joinGroup
    coordinator.joinGroup = async () => {
      coordinator.joinGroup = original
      throw error
    }

    const eachMessage = jest.fn()
    await consumer2.run({ eachMessage })

    const key = secureRandom()
    const message = { key: `key-${key}`, value: `value-${key}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId, restart: false },
    })
  })

  it('recovers from crashes due to retriable errors exhausting the number of retries', async () => {
    const groupId = `consumer-group-id-${secureRandom()}`
    consumer2 = createConsumer({
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
    consumer2.on(consumer.events.CRASH, crashListener)

    const cause = new KafkaJSError(new Error('💣'), { retriable: true })
    const retryError = new KafkaJSNumberOfRetriesExceeded(cause, {
      retryCount: 5,
      retryTime: 10000,
      cause,
    })
    const error = new KafkaJSNonRetriableError(retryError, { cause })

    await consumer2.connect()
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const original = coordinator.joinGroup
    coordinator.joinGroup = async () => {
      coordinator.joinGroup = original
      throw error
    }

    const eachMessage = jest.fn()
    await consumer2.run({ eachMessage })

    const key = secureRandom()
    const message = { key: `key-${key}`, value: `value-${key}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId, restart: true },
    })

    await expect(waitFor(() => eachMessage.mock.calls.length)).resolves.toBe(1)
  })

  it('recovers from retriable failures when "restartOnFailure" returns true', async () => {
    const errorMessage = '💣'
    let receivedErrorMessage
    const restartOnFailure = jest
      .fn()
      .mockImplementationOnce(async e => {
        receivedErrorMessage = e.message
        return true
      })
      .mockImplementationOnce(async () => false)

    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      retry: {
        retries: 0,
        initialRetryTime: 10,
        restartOnFailure,
      },
    })
    const crashListener = jest.fn()
    consumer.on(consumer.events.CRASH, crashListener)

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const runOpts = {
      eachMessage: async () => {
        throw new Error(errorMessage)
      },
    }
    jest.spyOn(runOpts, 'eachMessage')
    await consumer.run(runOpts)

    const key = secureRandom()
    const message = { key: `key-${key}`, value: `value-${key}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { groupId, error: expect.any(KafkaJSError), restart: true },
    })

    await waitFor(() => restartOnFailure.mock.calls.length > 0)
    expect(restartOnFailure).toHaveBeenCalledWith(expect.any(KafkaJSNumberOfRetriesExceeded))
    expect(receivedErrorMessage).toEqual(errorMessage)

    await expect(
      waitFor(() => runOpts.eachMessage.mock.calls.length)
    ).resolves.toBeGreaterThanOrEqual(1)
  })

  it('allows the user to bail out of restarting on retriable errors', async () => {
    const connectListener = jest.fn()
    const restartOnFailure = jest.fn(async () => {
      consumer.on(consumer.events.CONNECT, connectListener)
      return false
    })
    const initialRetryTime = 1
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      retry: {
        retries: 0,
        initialRetryTime,
        restartOnFailure,
      },
    })
    const crashListener = jest.fn()
    consumer.on(consumer.events.CRASH, crashListener)

    const error = new KafkaJSError(new Error('💣'), { retriable: true })

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const original = coordinator.joinGroup
    coordinator.joinGroup = async () => {
      coordinator.joinGroup = original
      throw error
    }

    const eachMessage = jest.fn()
    await consumer.run({ eachMessage })

    const key = secureRandom()
    const message = { key: `key-${key}`, value: `value-${key}` }
    await producer.send({ acks: 1, topic: topicName, messages: [message] })

    await waitFor(() => restartOnFailure.mock.calls.length > 0)
    expect(restartOnFailure).toHaveBeenCalledWith(error)

    // Very nasty, but it lets us assert that the consumer isn't restarting
    await sleep(initialRetryTime + 10)
    expect(connectListener).not.toHaveBeenCalled()
  })
})
