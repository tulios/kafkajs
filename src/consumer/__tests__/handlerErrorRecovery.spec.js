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

    await producer.connect()
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
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
        clientSideThrottleTime: expect.optional(0),
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

    it('does not commit the offset if autoCommit=false', async () => {
      let raisedError = false
      consumer.run({
        autoCommit: false,
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
        clientSideThrottleTime: expect.optional(0),
        throttleTime: 0,
        errorCode: 0,
        responses: [
          {
            partitions: [{ errorCode: 0, metadata: '', offset: '-1', partition: 0 }], // the offset stays the same
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
        clientSideThrottleTime: expect.optional(0),
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
