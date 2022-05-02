jest.setTimeout(30000)

const createAdmin = require('../../admin')
const createProducer = require('../../producer')
const createConsumer = require('../index')
const { Types } = require('../../protocol/message/compression')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
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
  testIfKafkaAtLeast_0_11,
  waitForConsumerToJoinGroup,
  generateMessages,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer, admin

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    admin = createAdmin({
      cluster,
      logger: newLogger(),
    })

    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  it('consume messages', async () => {
    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

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

    await waitForConsumerToJoinGroup(consumer)

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

  it('concurrent heartbeats are consolidated and respect heartbeatInterval', async () => {
    const partitionsConsumedConcurrently = 5
    const numberPartitions = 10
    const heartbeatInterval = 50
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 0,
      heartbeatInterval,
      logger: newLogger(),
    })
    topicName = `test-topic-${secureRandom()}`
    await createTopic({
      topic: topicName,
      partitions: numberPartitions,
    })
    await consumer.connect()
    await producer.connect()

    let then = Date.now()
    const heartbeats = []
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    consumer.on(consumer.events.HEARTBEAT, () => {
      const now = Date.now()
      heartbeats.push(now - then)
      then = now
    })

    const messagesConsumed = []
    consumer.run({
      partitionsConsumedConcurrently,
      eachBatch: async ({ batch: { messages }, heartbeat }) => {
        for (const event of messages) {
          await Promise.all([heartbeat(), heartbeat()])
          await sleep(1)
          messagesConsumed.push(event)
        }
      },
    })

    await waitForConsumerToJoinGroup(consumer)

    const messages = Array(200)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: messages.length })

    expect(messagesConsumed.length).toEqual(messages.length)
    for (const deltaTime of heartbeats) {
      expect(deltaTime).toBeGreaterThanOrEqual(heartbeatInterval)
    }
  })

  it('heartbeats are exposed in the eachMessage handler', async () => {
    consumer = createConsumer({
      cluster,
      groupId,
      heartbeatInterval: 50,
      logger: newLogger(),
    })

    topicName = `test-topic-${secureRandom()}`
    await createTopic({
      topic: topicName,
      partitions: 1,
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []

    let heartbeats = 0
    consumer.on(consumer.events.HEARTBEAT, () => {
      heartbeats++
    })

    consumer.run({
      eachMessage: async payload => {
        await new Promise(resolve => {
          setTimeout(resolve, 100)
        })

        await payload.heartbeat()
        messagesConsumed.push(payload.message)

        await new Promise(resolve => {
          setTimeout(resolve, 100)
        })
      },
    })

    await waitForConsumerToJoinGroup(consumer)

    await producer.send({ acks: 1, topic: topicName, messages: [{ key: 'value', value: 'value' }] })
    await waitForMessages(messagesConsumed, { number: 1 })

    expect(heartbeats).toBe(1)
  })

  it('consume GZIP messages', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const key1 = secureRandom()
    const message1 = { key: `key-${key1}`, value: `value-${key1}` }
    const key2 = secureRandom()
    const message2 = { key: `key-${key2}`, value: `value-${key2}` }

    await producer.send({
      acks: 1,
      topic: topicName,
      compression: Types.GZIP,
      messages: [message1, message2],
    })

    await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          offset: '0',
        }),
      }),
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message2.key),
          value: Buffer.from(message2.value),
          offset: '1',
        }),
      }),
    ])
  })

  it('consume batches', async () => {
    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const batchesConsumed = []
    const functionsExposed = []
    consumer.run({
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        isRunning,
        isStale,
        uncommittedOffsets,
      }) => {
        batchesConsumed.push(batch)
        functionsExposed.push(resolveOffset, heartbeat, isRunning, isStale, uncommittedOffsets)
      },
    })

    await waitForConsumerToJoinGroup(consumer)

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
      expect.any(Function),
      expect.any(Function),
    ])
  })

  it('commits the last offsets processed before stopping', async () => {
    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await Promise.all([admin.connect(), consumer.connect(), producer.connect()])
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    // stop the consumer right after processing the batch, the offsets should be
    // committed in the end
    consumer.on(consumer.events.END_BATCH_PROCESS, async () => {
      await consumer.stop()
    })

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
    const response = await admin.fetchOffsets({ groupId, topics: [topicName] })
    const { partitions } = response.find(({ topic }) => topic === topicName)
    const partition = partitions.find(({ partition }) => partition === 0)
    expect(partition.offset).toEqual('100') // check if offsets were committed
  })

  testIfKafkaAtLeast_0_11('consume messages with 0.11 format', async () => {
    const topicName2 = `test-topic2-${secureRandom()}`
    await createTopic({ topic: topicName2 })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    jest.spyOn(cluster, 'refreshMetadataIfNecessary')

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.subscribe({ topic: topicName2, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const generateMessagesWitHeaders = () =>
      generateMessages({ number: 103 }).map((message, i) => ({
        ...message,
        headers: {
          'header-keyA': `header-valueA-${i}`,
          'header-keyB': `header-valueB-${i}`,
          'header-keyC': `header-valueC-${i}`,
        },
      }))

    const messages1 = generateMessagesWitHeaders()
    const messages2 = generateMessagesWitHeaders()

    await producer.sendBatch({
      acks: 1,
      topicMessages: [
        { topic: topicName, messages: messages1 },
        { topic: topicName2, messages: messages2 },
      ],
    })

    await waitForMessages(messagesConsumed, { number: messages1.length + messages2.length })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()

    const messagesFromTopic1 = messagesConsumed.filter(m => m.topic === topicName)
    const messagesFromTopic2 = messagesConsumed.filter(m => m.topic === topicName2)

    expect(messagesFromTopic1[0]).toEqual(
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(messages1[0].key),
          value: Buffer.from(messages1[0].value),
          headers: {
            'header-keyA': Buffer.from(messages1[0].headers['header-keyA']),
            'header-keyB': Buffer.from(messages1[0].headers['header-keyB']),
            'header-keyC': Buffer.from(messages1[0].headers['header-keyC']),
          },
          magicByte: 2,
          offset: '0',
        }),
      })
    )

    const lastMessage1 = messages1[messages1.length - 1]
    expect(messagesFromTopic1[messagesFromTopic1.length - 1]).toEqual(
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(lastMessage1.key),
          value: Buffer.from(lastMessage1.value),
          headers: {
            'header-keyA': Buffer.from(lastMessage1.headers['header-keyA']),
            'header-keyB': Buffer.from(lastMessage1.headers['header-keyB']),
            'header-keyC': Buffer.from(lastMessage1.headers['header-keyC']),
          },
          magicByte: 2,
          offset: '102',
        }),
      })
    )

    expect(messagesFromTopic2[0]).toEqual(
      expect.objectContaining({
        topic: topicName2,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(messages2[0].key),
          value: Buffer.from(messages2[0].value),
          headers: {
            'header-keyA': Buffer.from(messages2[0].headers['header-keyA']),
            'header-keyB': Buffer.from(messages2[0].headers['header-keyB']),
            'header-keyC': Buffer.from(messages2[0].headers['header-keyC']),
          },
          magicByte: 2,
          offset: '0',
        }),
      })
    )

    const lastMessage2 = messages2[messages2.length - 1]
    expect(messagesFromTopic2[messagesFromTopic2.length - 1]).toEqual(
      expect.objectContaining({
        topic: topicName2,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(lastMessage2.key),
          value: Buffer.from(lastMessage2.value),
          headers: {
            'header-keyA': Buffer.from(lastMessage2.headers['header-keyA']),
            'header-keyB': Buffer.from(lastMessage2.headers['header-keyB']),
            'header-keyC': Buffer.from(lastMessage2.headers['header-keyC']),
          },
          magicByte: 2,
          offset: '102',
        }),
      })
    )

    // check if all offsets are present
    expect(messagesFromTopic1.map(m => m.message.offset)).toEqual(messages1.map((_, i) => `${i}`))
    expect(messagesFromTopic2.map(m => m.message.offset)).toEqual(messages2.map((_, i) => `${i}`))
  })

  testIfKafkaAtLeast_0_11('consume GZIP messages with 0.11 format', async () => {
    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    await consumer.connect()
    await producer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const key1 = secureRandom()
    const message1 = {
      key: `key-${key1}`,
      value: `value-${key1}`,
      headers: { [`header-${key1}`]: `header-value-${key1}` },
    }
    const key2 = secureRandom()
    const message2 = {
      key: `key-${key2}`,
      value: `value-${key2}`,
      headers: { [`header-${key2}`]: `header-value-${key2}` },
    }

    await producer.send({
      acks: 1,
      topic: topicName,
      compression: Types.GZIP,
      messages: [message1, message2],
    })

    await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message1.key),
          value: Buffer.from(message1.value),
          headers: {
            [`header-${key1}`]: Buffer.from(message1.headers[`header-${key1}`]),
          },
          magicByte: 2,
          offset: '0',
        }),
      }),
      expect.objectContaining({
        topic: topicName,
        partition: 0,
        message: expect.objectContaining({
          key: Buffer.from(message2.key),
          value: Buffer.from(message2.value),
          headers: {
            [`header-${key2}`]: Buffer.from(message2.headers[`header-${key2}`]),
          },
          magicByte: 2,
          offset: '1',
        }),
      }),
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

    await waitForConsumerToJoinGroup(consumer)

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
        groupId,
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
        groupId,
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
        eachBatch: async ({ batch, isStale, heartbeat, resolveOffset }) => {
          for (const message of batch.messages) {
            if (isStale()) break

            offsetsConsumed.push(message.offset)

            if (offsetsConsumed.length === 1) {
              consumer.seek({ topic: topicName, partition: 0, offset: message.offset })
            }

            resolveOffset(message.offset)
            await heartbeat()
          }
        },
      })

      await waitFor(() => offsetsConsumed.length >= 2, { delay: 50 })

      expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
    })

    it('skips messages fetched while seek was called', async () => {
      consumer = createConsumer({
        cluster: createCluster(),
        groupId,
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

      const eachBatch = async ({ batch, heartbeat }) => {
        for (const message of batch.messages) {
          offsetsConsumed.push(message.offset)
        }

        await heartbeat()
      }

      consumer.run({
        eachBatch,
      })

      await waitForConsumerToJoinGroup(consumer)

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
      groupId,
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

    const eachBatch = async ({ batch, heartbeat }) => {
      for (const message of batch.messages) {
        offsetsConsumed.push(message.offset)
      }

      await heartbeat()
    }

    consumer.run({
      eachBatch,
    })

    await waitForConsumerToJoinGroup(consumer)
    await waitFor(() => offsetsConsumed.length === messages.length, { delay: 50 })
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    consumer.pause([{ topic: topicName }])
    await producer.send({ acks: 1, topic: topicName, messages }) // trigger completion of fetch

    await waitForNextEvent(consumer, consumer.events.FETCH)

    expect(offsetsConsumed.length).toEqual(messages.length)
  })

  describe('transactions', () => {
    testIfKafkaAtLeast_0_11('accepts messages from an idempotent producer', async () => {
      cluster = createCluster()
      producer = createProducer({
        cluster,
        createPartitioner: createModPartitioner,
        logger: newLogger(),
        transactionalId: `transactional-id-${secureRandom()}`,
        idempotent: true,
        maxInFlightRequests: 1,
      })

      consumer = createConsumer({
        cluster,
        groupId,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })

      jest.spyOn(cluster, 'refreshMetadataIfNecessary')

      await consumer.connect()
      await producer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []
      const idempotentMessages = generateMessages({ prefix: 'idempotent' })

      consumer.run({
        eachMessage: async event => messagesConsumed.push(event),
      })
      await waitForConsumerToJoinGroup(consumer)

      await producer.sendBatch({
        topicMessages: [{ topic: topicName, messages: idempotentMessages }],
      })

      const number = idempotentMessages.length
      await waitForMessages(messagesConsumed, {
        number,
      })

      expect(messagesConsumed).toHaveLength(idempotentMessages.length)
      expect(messagesConsumed[0].message.value.toString()).toMatch(/value-idempotent-0/)
      expect(messagesConsumed[99].message.value.toString()).toMatch(/value-idempotent-99/)
    })

    testIfKafkaAtLeast_0_11('accepts messages from committed transactions', async () => {
      cluster = createCluster()
      producer = createProducer({
        cluster,
        createPartitioner: createModPartitioner,
        logger: newLogger(),
        transactionalId: `transactional-id-${secureRandom()}`,
        maxInFlightRequests: 1,
      })

      consumer = createConsumer({
        cluster,
        groupId,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })

      jest.spyOn(cluster, 'refreshMetadataIfNecessary')

      await consumer.connect()
      await producer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []

      const messages1 = generateMessages({ prefix: 'txn1' })
      const messages2 = generateMessages({ prefix: 'txn2' })
      const nontransactionalMessages1 = generateMessages({ prefix: 'nontransactional1', number: 1 })
      const nontransactionalMessages2 = generateMessages({ prefix: 'nontransactional2', number: 1 })

      consumer.run({
        eachMessage: async event => messagesConsumed.push(event),
      })
      await waitForConsumerToJoinGroup(consumer)

      // We can send non-transaction messages
      await producer.sendBatch({
        topicMessages: [{ topic: topicName, messages: nontransactionalMessages1 }],
      })

      // We can run a transaction
      const txn1 = await producer.transaction()
      await txn1.sendBatch({
        topicMessages: [{ topic: topicName, messages: messages1 }],
      })
      await txn1.commit()

      // We can immediately run another transaction
      const txn2 = await producer.transaction()
      await txn2.sendBatch({
        topicMessages: [{ topic: topicName, messages: messages2 }],
      })
      await txn2.commit()

      // We can return to sending non-transaction messages
      await producer.sendBatch({
        topicMessages: [{ topic: topicName, messages: nontransactionalMessages2 }],
      })

      const number =
        messages1.length +
        messages2.length +
        nontransactionalMessages1.length +
        nontransactionalMessages2.length
      await waitForMessages(messagesConsumed, {
        number,
      })

      expect(messagesConsumed[0].message.value.toString()).toMatch(/value-nontransactional1-0/)
      expect(messagesConsumed[1].message.value.toString()).toMatch(/value-txn1-0/)
      expect(messagesConsumed[number - 1].message.value.toString()).toMatch(
        /value-nontransactional2-0/
      )
      expect(messagesConsumed[number - 2].message.value.toString()).toMatch(/value-txn2-99/)
    })

    testIfKafkaAtLeast_0_11('does not receive aborted messages', async () => {
      cluster = createCluster()
      producer = createProducer({
        cluster,
        createPartitioner: createModPartitioner,
        logger: newLogger(),
        transactionalId: `transactional-id-${secureRandom()}`,
        maxInFlightRequests: 1,
      })

      consumer = createConsumer({
        cluster,
        groupId,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })

      jest.spyOn(cluster, 'refreshMetadataIfNecessary')

      await consumer.connect()
      await producer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      const messagesConsumed = []

      const abortedMessages1 = generateMessages({ prefix: 'aborted-txn-1' })
      const abortedMessages2 = generateMessages({ prefix: 'aborted-txn-2' })
      const nontransactionalMessages = generateMessages({ prefix: 'nontransactional', number: 1 })
      const committedMessages = generateMessages({ prefix: 'committed-txn', number: 10 })

      consumer.run({
        eachMessage: async event => messagesConsumed.push(event),
      })
      await waitForConsumerToJoinGroup(consumer)

      const abortedTxn1 = await producer.transaction()
      await abortedTxn1.sendBatch({
        topicMessages: [{ topic: topicName, messages: abortedMessages1 }],
      })
      await abortedTxn1.abort()

      await producer.sendBatch({
        topicMessages: [{ topic: topicName, messages: nontransactionalMessages }],
      })

      const abortedTxn2 = await producer.transaction()
      await abortedTxn2.sendBatch({
        topicMessages: [{ topic: topicName, messages: abortedMessages2 }],
      })
      await abortedTxn2.abort()

      const committedTxn = await producer.transaction()
      await committedTxn.sendBatch({
        topicMessages: [{ topic: topicName, messages: committedMessages }],
      })
      await committedTxn.commit()

      const number = nontransactionalMessages.length + committedMessages.length
      await waitForMessages(messagesConsumed, {
        number,
      })

      expect(messagesConsumed).toHaveLength(11)
      expect(messagesConsumed[0].message.value.toString()).toMatch(/value-nontransactional-0/)
      expect(messagesConsumed[1].message.value.toString()).toMatch(/value-committed-txn-0/)
      expect(messagesConsumed[10].message.value.toString()).toMatch(/value-committed-txn-9/)
    })

    testIfKafkaAtLeast_0_11(
      'receives aborted messages for an isolation level of READ_UNCOMMITTED',
      async () => {
        const isolationLevel = ISOLATION_LEVEL.READ_UNCOMMITTED

        cluster = createCluster({ isolationLevel })
        producer = createProducer({
          cluster,
          createPartitioner: createModPartitioner,
          logger: newLogger(),
          transactionalId: `transactional-id-${secureRandom()}`,
          maxInFlightRequests: 1,
        })

        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 100,
          logger: newLogger(),
          isolationLevel,
        })

        jest.spyOn(cluster, 'refreshMetadataIfNecessary')

        await consumer.connect()
        await producer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const messagesConsumed = []

        const abortedMessages = generateMessages({ prefix: 'aborted-txn1' })

        consumer.run({
          eachMessage: async event => messagesConsumed.push(event),
        })
        await waitForConsumerToJoinGroup(consumer)

        const abortedTxn1 = await producer.transaction()
        await abortedTxn1.sendBatch({
          topicMessages: [{ topic: topicName, messages: abortedMessages }],
        })
        await abortedTxn1.abort()

        const number = 1
        await waitForMessages(messagesConsumed, {
          number,
        })

        expect(messagesConsumed).toHaveLength(abortedMessages.length)
        expect(messagesConsumed[0].message.value.toString()).toMatch(/value-aborted-txn1-0/)
        expect(messagesConsumed[messagesConsumed.length - 1].message.value.toString()).toMatch(
          /value-aborted-txn1-99/
        )
      }
    )

    testIfKafkaAtLeast_0_11(
      'respects offsets sent by a committed transaction ("consume-transform-produce" flow)',
      async () => {
        cluster = createCluster()
        producer = createProducer({
          cluster,
          logger: newLogger(),
          transactionalId: `transactional-id-${secureRandom()}`,
          maxInFlightRequests: 1,
        })

        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 100,
          logger: newLogger(),
        })

        await consumer.connect()
        await producer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        // 1. Run consumer with "autoCommit=false"

        let messagesConsumed = []
        let uncommittedOffsetsPerMessage = []
        let getCurrentUncommittedOffsets

        const eachBatch = async ({ batch, uncommittedOffsets, heartbeat, resolveOffset }) => {
          for (const message of batch.messages) {
            messagesConsumed.push(message)
            resolveOffset(message.offset)
            uncommittedOffsetsPerMessage.push(uncommittedOffsets())
            getCurrentUncommittedOffsets = uncommittedOffsets
          }

          await heartbeat()
        }

        consumer.run({
          autoCommit: false,
          eachBatch,
        })
        await waitForConsumerToJoinGroup(consumer)

        // 2. Produce messages and consume

        const partition = 0
        const messages = generateMessages().map(message => ({
          ...message,
          partition,
        }))
        await producer.send({
          acks: 1,
          topic: topicName,
          messages,
        })

        const number = messages.length
        await waitForMessages(messagesConsumed, {
          number,
        })

        expect(messagesConsumed[0].value.toString()).toMatch(/value-0/)
        expect(messagesConsumed[99].value.toString()).toMatch(/value-99/)
        expect(uncommittedOffsetsPerMessage).toHaveLength(messagesConsumed.length)

        // 3. Send offsets in a transaction and commit
        const txnToCommit = await producer.transaction()
        await txnToCommit.sendOffsets({
          consumerGroupId: groupId,
          topics: uncommittedOffsetsPerMessage[97].topics,
        })
        await txnToCommit.commit()

        // Restart consumer
        await consumer.stop()
        messagesConsumed = []
        uncommittedOffsetsPerMessage = []

        consumer.run({ eachBatch, autoCommit: false })

        // Assert we only consume the messages that were after the sent offset
        await waitForMessages(messagesConsumed, {
          number: 2,
        })

        expect(messagesConsumed).toHaveLength(2)
        expect(messagesConsumed[0].value.toString()).toMatch(/value-98/)
        expect(messagesConsumed[1].value.toString()).toMatch(/value-99/)

        const lastUncommittedOffsets = uncommittedOffsetsPerMessage.pop()
        expect(getCurrentUncommittedOffsets()).toEqual(lastUncommittedOffsets)
        expect(getCurrentUncommittedOffsets()).toEqual({
          topics: [
            {
              topic: topicName,
              partitions: [{ partition: partition.toString(), offset: '100' }],
            },
          ],
        })

        const txn2ToCommit = await producer.transaction()
        await txn2ToCommit.sendOffsets({
          consumerGroupId: groupId,
          topics: lastUncommittedOffsets.topics,
        })
        await txn2ToCommit.commit()

        expect(getCurrentUncommittedOffsets()).toEqual({
          topics: [],
        })
      }
    )

    testIfKafkaAtLeast_0_11(
      'does not respect offsets sent by an aborted transaction ("consume-transform-produce" flow)',
      async () => {
        cluster = createCluster({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
        producer = createProducer({
          cluster,
          logger: newLogger(),
          transactionalId: `transactional-id-${secureRandom()}`,
          maxInFlightRequests: 1,
        })

        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 100,
          logger: newLogger(),
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })

        await consumer.connect()
        await producer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        // 1. Run consumer with "autoCommit=false"

        let messagesConsumed = []
        let uncommittedOffsetsPerMessage = []
        let getCurrentUncommittedOffsets

        const eachBatch = async ({ batch, uncommittedOffsets, heartbeat, resolveOffset }) => {
          for (const message of batch.messages) {
            messagesConsumed.push(message)
            resolveOffset(message.offset)
            uncommittedOffsetsPerMessage.push(uncommittedOffsets())
            getCurrentUncommittedOffsets = uncommittedOffsets
          }

          await heartbeat()
        }

        consumer.run({
          autoCommit: false,
          eachBatch,
        })
        await waitForConsumerToJoinGroup(consumer)

        // 2. Produce messages and consume

        const partition = 0
        const messages = generateMessages().map(message => ({
          ...message,
          partition,
        }))
        await producer.send({
          acks: 1,
          topic: topicName,
          messages,
        })

        await waitFor(() => messagesConsumed.length >= messages.length, {
          ignoreTimeout: false,
          timeoutMessage: `Failed to consume all produced messages`,
        })
        await consumer.stop()

        expect(messagesConsumed[0].value.toString()).toMatch(/value-0/)
        expect(messagesConsumed[99].value.toString()).toMatch(/value-99/)
        expect(uncommittedOffsetsPerMessage).toHaveLength(messagesConsumed.length)

        // 3. Send offsets in a transaction and abort
        const txnToAbort = await producer.transaction()
        await txnToAbort.sendOffsets({
          consumerGroupId: groupId,
          topics: uncommittedOffsetsPerMessage[98].topics,
        })
        await txnToAbort.abort()

        // Restart consumer
        messagesConsumed = []
        uncommittedOffsetsPerMessage = []

        consumer.run({
          autoCommit: false,
          eachBatch,
        })

        await waitFor(() => messagesConsumed.length >= 1, {
          ignoreTimeout: false,
          timeoutMessage: 'Failed to consume any messages after transaction was aborted',
        })

        expect(messagesConsumed[0].value.toString()).toMatch(/value-0/)

        await waitFor(() => messagesConsumed.length >= messages.length, {
          ignoreTimeout: false,
          timeoutMessage: 'Failed to consume all messages after transaction was aborted',
        })

        expect(messagesConsumed[messagesConsumed.length - 1].value.toString()).toMatch(/value-99/)

        const lastUncommittedOffsets = uncommittedOffsetsPerMessage.pop()
        expect(getCurrentUncommittedOffsets()).toEqual(lastUncommittedOffsets)
        expect(getCurrentUncommittedOffsets()).toEqual({
          topics: [
            { topic: topicName, partitions: [{ partition: partition.toString(), offset: '100' }] },
          ],
        })
      }
    )
  })
})
