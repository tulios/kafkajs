const createProducer = require('../../producer')
const createManualConsumer = require('../index')
const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
} = require('testHelpers')

describe('ManualConsumer', () => {
  let topicName, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createManualConsumer({
      cluster,
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
      await expect(waitFor(() => succeeded)).resolves.toBeTruthy()

      // retry the same message
      expect(messages.map(m => m.offset)).toEqual(['0', '0'])
      expect(messages.map(m => m.key.toString())).toEqual([`key-${key1}`, `key-${key1}`])
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

      await expect(waitFor(() => succeeded)).resolves.toBeTruthy()

      // retry the same batch
      expect(batches.map(b => b.messages.map(m => m.offset).join(','))).toEqual(['0,1,2', '0,1,2'])
      const batchMessages = batches.map(b => b.messages.map(m => m.key.toString()).join('-'))
      expect(batchMessages).toEqual([
        `key-${key1}-key-${key2}-key-${key3}`,
        `key-${key1}-key-${key2}-key-${key3}`,
      ])
    })
  })
})
