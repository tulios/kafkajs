const createProducer = require('../../producer')
const createConsumer = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')
const { KafkaJSError } = require('../../errors')

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
  let topicName, groupId, cluster, producer, consumer

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
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  it('recovers from offset out of range', async () => {
    await consumer.connect()
    await producer.connect()

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const { generationId, memberId } = await coordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
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
      {
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      },
    ])
  })

  it('recovers from crashes due to retriable errors', async () => {
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

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId },
    })

    await expect(waitFor(() => eachMessage.mock.calls.length)).resolves.toBe(1)
  })

  it('recovers from retriable failures when "restartOnFailure" returns true', async () => {
    const restartOnFailure = jest.fn(async () => true)
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      retry: {
        retries: 0,
        initialRetryTime: 1,
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

    await waitFor(() => crashListener.mock.calls.length > 0)
    expect(crashListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'consumer.crash',
      payload: { error, groupId },
    })

    await waitFor(() => restartOnFailure.mock.calls.length > 0)
    expect(restartOnFailure).toHaveBeenCalledWith(error)

    await expect(waitFor(() => eachMessage.mock.calls.length)).resolves.toBeGreaterThanOrEqual(1)
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

  describe('when eachMessage throws an error', () => {
    let key1, key3

    beforeEach(async () => {
      await consumer.connect()
      await producer.connect()

      key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      const key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ acks: 1, topic: topicName, messages: [message1, message2, message3] })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
    })

    it('retries the same message', async () => {
      let succeeded = false
      const messages = []
      const eachMessage = jest
        .fn()
        .mockImplementationOnce(({ message }) => {
          messages.push(message)
          throw new Error('Fail once')
        })
        .mockImplementationOnce(({ message }) => {
          messages.push(message)
          succeeded = true
        })

      consumer.run({ eachMessage })
      await waitForConsumerToJoinGroup(consumer)
      await expect(waitFor(() => succeeded)).resolves.toBeTruthy()

      // retry the same message
      expect(messages.map(m => m.offset)).toEqual(['0', '0'])
      expect(messages.map(m => m.key.toString())).toEqual([`key-${key1}`, `key-${key1}`])
    })

    it('commits the previous offsets', async () => {
      let raisedError = false
      consumer.run({
        eachMessage: async event => {
          if (event.message.key.toString() === `key-${key3}`) {
            raisedError = true
            throw new Error('some error')
          }
        },
      })

      await waitForConsumerToJoinGroup(consumer)
      await expect(waitFor(() => raisedError)).resolves.toBeTruthy()
      const coordinator = await cluster.findGroupCoordinator({ groupId })
      const offsets = await coordinator.offsetFetch({
        groupId,
        topics: [
          {
            topic: topicName,
            partitions: [{ partition: 0 }],
          },
        ],
      })

      expect(offsets).toEqual({
        throttleTime: 0,
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '2', partition: 0 }],
            topic: topicName,
          },
        ],
      })
    })
  })

  describe('when eachBatch throws an error', () => {
    let key1, key2, key3

    beforeEach(async () => {
      await consumer.connect()
      await producer.connect()

      key1 = secureRandom()
      const message1 = { key: `key-${key1}`, value: `value-${key1}` }
      key2 = secureRandom()
      const message2 = { key: `key-${key2}`, value: `value-${key2}` }
      key3 = secureRandom()
      const message3 = { key: `key-${key3}`, value: `value-${key3}` }

      await producer.send({ acks: 1, topic: topicName, messages: [message1, message2, message3] })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
    })

    it('retries the same batch', async () => {
      let succeeded = false
      const batches = []
      const eachBatch = jest
        .fn()
        .mockImplementationOnce(({ batch }) => {
          batches.push(batch)
          throw new Error('Fail once')
        })
        .mockImplementationOnce(({ batch }) => {
          batches.push(batch)
          succeeded = true
        })

      consumer.run({ eachBatch })

      await waitForConsumerToJoinGroup(consumer)
      await expect(waitFor(() => succeeded)).resolves.toBeTruthy()

      // retry the same batch
      expect(batches.map(b => b.messages.map(m => m.offset).join(','))).toEqual(['0,1,2', '0,1,2'])
      const batchMessages = batches.map(b => b.messages.map(m => m.key.toString()).join('-'))
      expect(batchMessages).toEqual([
        `key-${key1}-key-${key2}-key-${key3}`,
        `key-${key1}-key-${key2}-key-${key3}`,
      ])
    })

    it('commits the previous offsets', async () => {
      let raisedError = false
      consumer.run({
        eachBatch: async ({ batch, resolveOffset }) => {
          for (const message of batch.messages) {
            if (message.key.toString() === `key-${key3}`) {
              raisedError = true
              throw new Error('some error')
            }
            resolveOffset(message.offset)
          }
        },
      })

      await waitForConsumerToJoinGroup(consumer)
      await expect(waitFor(() => raisedError)).resolves.toBeTruthy()
      const coordinator = await cluster.findGroupCoordinator({ groupId })
      const offsets = await coordinator.offsetFetch({
        groupId,
        topics: [
          {
            topic: topicName,
            partitions: [{ partition: 0 }],
          },
        ],
      })

      expect(offsets).toEqual({
        throttleTime: 0,
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '2', partition: 0 }],
            topic: topicName,
          },
        ],
      })
    })
  })
})
