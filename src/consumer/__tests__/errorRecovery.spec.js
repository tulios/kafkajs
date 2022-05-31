const createProducer = require('../../producer')
const createConsumer = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSNonRetriableError,
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
