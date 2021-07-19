jest.setTimeout(10000)

const createProducer = require('../../producer')
const createConsumer = require('../index')
const sleep = require('../../utils/sleep')
const { KafkaJSError } = require('../../errors')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForNextEvent,
  generateMessages,
} = require('testHelpers')

let topicName, groupId, cluster, consumer, partitionProcessedBatchCount, restartOnFailure
const numBatches = 3

const incBatchCountAfter = delay => async ({ batch: { partition } }) => {
  if (delay) await sleep(delay)
  partitionProcessedBatchCount[partition]++
}

const throwRetriableErrorAfter = delay => async () => {
  await sleep(delay)
  throw new KafkaJSError(new Error('💣'), { retriable: true })
}

const throwNoneRetriableErrorAfter = delay => async () => {
  await sleep(delay)
  throw new Error(new Error('💣'))
}

beforeAll(async () => {
  topicName = `test-topic-${secureRandom()}`
  await createTopic({
    topic: topicName,
    partitions: numBatches,
  })
  cluster = createCluster()
  const producer = createProducer({
    cluster,
    createPartitioner: createModPartitioner,
    logger: newLogger(),
  })
  await producer.connect()
  await producer.send({
    topic: topicName,
    messages: generateMessages({ number: numBatches * 10 }),
  })
  await producer.disconnect()
})

beforeEach(async () => {
  restartOnFailure = jest.fn(() => false)
  groupId = `consumer-group-id-${secureRandom()}`
  cluster = createCluster()
  consumer = createConsumer({
    cluster,
    groupId,
    maxWaitTimeInMs: 10,
    logger: newLogger(),
    retry: {
      retries: 1,
      initialRetryTime: 10,
      restartOnFailure,
    },
  })
  await consumer.connect()
  await consumer.subscribe({ topic: topicName, fromBeginning: true })

  partitionProcessedBatchCount = Array(numBatches).fill(0)
})

afterEach(async () => {
  consumer && (await consumer.disconnect())
})

describe('After a none-retriable batch processing error', () => {
  it('with a consumer set to not restart, aborts pending batch processing and exits', async () => {
    restartOnFailure.mockImplementation(() => false)

    const eachBatch = jest
      .fn()
      .mockImplementation(incBatchCountAfter())
      .mockImplementationOnce(throwNoneRetriableErrorAfter(2))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachBatch,
    })

    await Promise.all([
      waitForNextEvent(consumer, consumer.events.CRASH),
      waitForNextEvent(consumer, consumer.events.STOP),
    ])

    await consumer.disconnect()

    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(1)
  })

  it('with a consumer set to restart, processes the batches after restarting', async () => {
    restartOnFailure.mockImplementation(() => true)
    const eachBatch = jest
      .fn()
      .mockImplementation(incBatchCountAfter())
      .mockImplementationOnce(throwNoneRetriableErrorAfter(2))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.CRASH)

    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()
    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numbatches + 1 error case
  })
})

describe('After a retriable batch processing error', () => {
  it('aborts pending queued batch processing before retrying the whole fetch', async () => {
    const eachBatch = jest
      .fn()
      .mockImplementation(incBatchCountAfter())
      .mockImplementationOnce(throwRetriableErrorAfter())

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numbatches + 1 error case
  })

  it('completes in-progress batch processing before retrying the entire fetch', async () => {
    const eachBatch = jest
      .fn()
      .mockImplementation(incBatchCountAfter(10))
      .mockImplementationOnce(throwRetriableErrorAfter(2))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: numBatches,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    expect(partitionProcessedBatchCount.filter(n => n === 1).length).toEqual(numBatches - 1)

    await waitForNextEvent(consumer, consumer.events.FETCH_START)

    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numBatches + 1 error
  })
})
