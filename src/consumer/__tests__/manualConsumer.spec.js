jest.setTimeout(30000)

const createProducer = require('../../producer')
const createConsumer = require('../../consumer/index')
const sleep = require('../../utils/sleep')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitFor,
  waitForMessages,
  waitForNextEvent,
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

    consumer = createConsumer({
      cluster,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  it('consumes messages', async () => {
    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

    const messages = Array(100)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()

    expect(messagesConsumed[0]).toEqual(
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(messages[0].key),
          value: Buffer.from(messages[0].value),
          offset: '0',
        }),
      })
    )

    expect(messagesConsumed[messagesConsumed.length - 1]).toEqual(
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(messages[messages.length - 1].key),
          value: Buffer.from(messages[messages.length - 1].value),
          offset: '99',
        }),
      })
    )

    // check if all offsets are present
    expect(messagesConsumed.map(m => m.message.offset)).toEqual(messages.map((_, i) => `${i}`))
  })

  it('consumes messages concurrently', async () => {
    const partitionsConsumedConcurrently = 2
    topicName = `test-topic-${secureRandom()}`
    await createTopic({
      topic: topicName,
      partitions: partitionsConsumedConcurrently + 1,
    })
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    let inProgress = 0
    let hitConcurrencyLimit = false
    consumer.on(consumer.events.START_BATCH_PROCESS, () => {
      inProgress++
      expect(inProgress).toBeLessThanOrEqual(partitionsConsumedConcurrently)
      hitConcurrencyLimit = hitConcurrencyLimit || inProgress === partitionsConsumedConcurrently
    })
    consumer.on(consumer.events.END_BATCH_PROCESS, () => inProgress--)

    const messagesConsumed = []
    consumer.run({
      partitionsConsumedConcurrently,
      eachMessage: async event => {
        await sleep(1)
        messagesConsumed.push(event)
      },
    })

    const messages = Array(100)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(hitConcurrencyLimit).toBeTrue()
  })

  it('consumes batches', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const batchesConsumed = []
    const functionsExposed = []
    consumer.run({
      eachBatch: async ({ batch, resolveOffset, isRunning, isStale }) => {
        batchesConsumed.push(batch)
        functionsExposed.push(resolveOffset, isRunning, isStale)
      },
    })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ acks: 1, topic: topicName, messages: [message1, message2] })

    await expect(waitForMessages(batchesConsumed)).resolves.toEqual([
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        highWatermark: '2',
        messages: [
          expect.objectContaining({
            key: Buffer.from(message1.key),
            value: Buffer.from(message1.value),
            offset: '0',
          }),
          expect.objectContaining({
            key: Buffer.from(message2.key),
            value: Buffer.from(message2.value),
            offset: '1',
          }),
        ],
      }),
    ])

    expect(functionsExposed).toEqual([
      expect.any(Function),
      expect.any(Function),
      expect.any(Function),
    ])
  })

  it('stops consuming messages when running = false', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    let calls = 0

    consumer.run({
      eachMessage: async event => {
        calls++
        await sleep(100)
      },
    })

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({ acks: 1, topic: topicName, messages: [message1, message2] })
    await waitFor(() => calls > 0, {})
    await consumer.disconnect() // don't give the consumer the chance to consume the 2nd message

    expect(calls).toEqual(1)
  })

  describe('discarding messages after seeking', () => {
    it('stops consuming messages when fetched batch has gone stale', async () => {
      consumer = createConsumer({
        cluster: createCluster(),
        logger: newLogger(),

        // make sure we fetch a batch of messages
        minBytes: 1024,
        maxWaitTimeInMs: 500,
      })

      const messages = Array(10)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await consumer.connect()
      await producer.connect()
      await producer.send({ acks: 1, topic: topicName, messages })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const offsetsConsumed = []

      consumer.run({
        eachMessage: async ({ message }) => {
          offsetsConsumed.push(message.offset)

          if (offsetsConsumed.length === 1) {
            consumer.seek({ topic: topicName, partition: 0, offset: message.offset })
          }
        },
      })

      await waitFor(() => offsetsConsumed.length >= 2, { delay: 50 })

      expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
    })

    it('resolves a batch as stale when seek was called while processing it', async () => {
      consumer = createConsumer({
        cluster: createCluster(),
        logger: newLogger(),

        // make sure we fetch a batch of messages
        minBytes: 1024,
        maxWaitTimeInMs: 500,
      })

      const messages = Array(10)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })

      await consumer.connect()
      await producer.connect()
      await producer.send({ acks: 1, topic: topicName, messages })
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const offsetsConsumed = []

      consumer.run({
        eachBatch: async ({ batch, isStale, resolveOffset }) => {
          for (const message of batch.messages) {
            if (isStale()) break

            offsetsConsumed.push(message.offset)

            if (offsetsConsumed.length === 1) {
              consumer.seek({ topic: topicName, partition: 0, offset: message.offset })
            }

            resolveOffset(message.offset)
          }
        },
      })

      await waitFor(() => offsetsConsumed.length >= 2, { delay: 50 })

      expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
    })

    it('skips messages fetched while seek was called', async () => {
      consumer = createConsumer({
        cluster: createCluster(),
        maxWaitTimeInMs: 1000,
        logger: newLogger(),
      })

      const messages = Array(10)
        .fill()
        .map(() => {
          const value = secureRandom()
          return { key: `key-${value}`, value: `value-${value}` }
        })
      await producer.connect()
      await producer.send({ acks: 1, topic: topicName, messages })

      await consumer.connect()

      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const offsetsConsumed = []

      const eachBatch = async ({ batch }) => {
        for (const message of batch.messages) {
          offsetsConsumed.push(message.offset)
        }
      }

      consumer.run({
        eachBatch,
      })

      await waitFor(() => offsetsConsumed.length === messages.length, { delay: 50 })
      await waitForNextEvent(consumer, consumer.events.FETCH_START)

      const seekedOffset = offsetsConsumed[Math.floor(messages.length / 2)]
      consumer.seek({ topic: topicName, partition: 0, offset: seekedOffset })
      await producer.send({ acks: 1, topic: topicName, messages }) // trigger completion of fetch

      await waitFor(() => offsetsConsumed.length > messages.length, { delay: 50 })

      expect(offsetsConsumed[messages.length]).toEqual(seekedOffset)
    })
  })

  it('discards messages received when pausing while fetch is in-flight', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      maxWaitTimeInMs: 200,
      logger: newLogger(),
    })

    const messages = Array(10)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })
    await producer.connect()
    await producer.send({ acks: 1, topic: topicName, messages })

    await consumer.connect()

    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const offsetsConsumed = []

    const eachBatch = async ({ batch }) => {
      for (const message of batch.messages) {
        offsetsConsumed.push(message.offset)
      }
    }

    consumer.run({
      eachBatch,
    })

    await waitFor(() => offsetsConsumed.length === messages.length, { delay: 50 })
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    consumer.pause([{ topic: topicName }])
    await producer.send({ acks: 1, topic: topicName, messages }) // trigger completion of fetch

    await waitForNextEvent(consumer, consumer.events.FETCH)

    expect(offsetsConsumed.length).toEqual(messages.length)
  })

  it('seek updates partition offsets', async () => {
    topicName = `test-topic-${secureRandom()}`
    await createTopic({ topic: topicName, partitions: 2 })

    await consumer.connect()
    await producer.connect()

    const value1 = secureRandom()
    const message1 = { key: `key-1`, value: `value-${value1}` }
    const value2 = secureRandom()
    const message2 = { key: `key-1`, value: `value-${value2}` }
    const value3 = secureRandom()
    const message3 = { key: `key-0`, value: `value-${value3}` }
    const value4 = secureRandom()
    const message4 = { key: `key-0`, value: `value-${value4}` }
    const value5 = secureRandom()
    const message5 = { key: `key-0`, value: `value-${value5}` }

    await producer.send({
      acks: 1,
      topic: topicName,
      messages: [message1, message2, message3, message4, message5],
    })
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    consumer.seek({ topic: topicName, partition: 0, offset: 2 })
    consumer.seek({ topic: topicName, partition: 1, offset: 1 })

    await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          topic: topicName,
          partition: 0,
          message: expect.objectContaining({ offset: '2' }),
        }),
        expect.objectContaining({
          topic: topicName,
          partition: 1,
          message: expect.objectContaining({ offset: '1' }),
        }),
      ])
    )
  })

  it('subscribes by topic name as a string or regex', async () => {
    const testScope = secureRandom()
    const regexMatchingTopic = `pattern-${testScope}-regex-${secureRandom()}`
    const topics = [`topic-${secureRandom()}`, `topic-${secureRandom()}`, regexMatchingTopic]

    await Promise.all(topics.map(topic => createTopic({ topic })))

    const messagesConsumed = []
    await consumer.connect()
    await consumer.subscribe({
      topics: [topics[0], topics[1], new RegExp(`pattern-${testScope}-regex-.*`, 'i')],
      fromBeginning: true,
    })

    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })

    await producer.connect()
    await producer.sendBatch({
      acks: 1,
      topicMessages: [
        { topic: topics[0], messages: [{ key: 'drink', value: 'drink' }] },
        { topic: topics[1], messages: [{ key: 'your', value: 'your' }] },
        { topic: topics[2], messages: [{ key: 'ovaltine', value: 'ovaltine' }] },
      ],
    })

    await waitForMessages(messagesConsumed, { number: 3 })
    expect(messagesConsumed.map(m => m.message.value.toString())).toEqual(
      expect.arrayContaining(['drink', 'your', 'ovaltine'])
    )
  })
})
