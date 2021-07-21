jest.setTimeout(10000)

const createProducer = require('../../producer')
const createConsumer = require('../index')
const sleep = require('../../utils/sleep')
const { KafkaJSError, KafkaJSAggregateError } = require('../../errors')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForNextEvent,
  generateMessages,
} = require('testHelpers')
const { describe } = require('jest-circus')

let topicName, groupId, cluster, consumer, partitionProcessedBatchCount, restartOnFailure

const numBrokers = 3
const numBatchesPerBroker = 2
const numBatches = numBrokers * numBatchesPerBroker

const retriableError = new KafkaJSError(new Error('💣'), { retriable: true })
const noneRetriableError = new Error(new Error('💣'))

const incBatchCountAfter = delay => async ({ batch: { partition } }) => {
  if (delay) await sleep(delay)
  partitionProcessedBatchCount[partition]++
}

const throwRetriableErrorAfter = delay => async () => {
  if (delay) await sleep(delay)
  throw retriableError
}

const throwNoneRetriableErrorAfter = delay => async () => {
  if (delay) await sleep(delay)
  throw noneRetriableError
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
    numBatchesPerBroker: 10485760,
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
      .fn(incBatchCountAfter())
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
    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()

    await consumer.disconnect()
    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(1)
  })

  it('with a consumer set to restart, aborts pending batch processing, restarts, then re-fetches and processes the batches', async () => {
    restartOnFailure.mockImplementation(() => true)
    const eachBatch = jest
      .fn(incBatchCountAfter())
      .mockImplementationOnce(throwNoneRetriableErrorAfter(2))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.CRASH)
    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.every(n => n === 0)).toBeTrue()

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()

    await consumer.disconnect()
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numbatches + 1 error case
  })
})

describe('After a retriable batch processing error', () => {
  it('aborts pending queued batch processing before retrying the fetch', async () => {
    const eachBatch = jest
      .fn(incBatchCountAfter())
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

    await consumer.disconnect()
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numbatches + 1 error case
  })

  it('completes in-progress batch processing before retrying the fetch', async () => {
    const eachBatch = jest
      .fn(incBatchCountAfter(20))
      .mockImplementationOnce(throwRetriableErrorAfter(5))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: numBatches,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.filter(n => n === 1)).toHaveLength(numBatches - 1)

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()

    await consumer.disconnect()
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches + 1) // numBatches + 1 error
  })
})

describe('After a retriable broker fetch error', () => {
  it('completes all other batch processing before retrying the fetch', async () => {
    const broker = await cluster.findBroker({ nodeId: 0 })
    const brokerFetchRequest = jest.spyOn(broker, 'fetch')

    brokerFetchRequest.mockImplementationOnce(throwRetriableErrorAfter(2))

    const eachBatch = jest.fn(incBatchCountAfter(10))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: numBatches,
      eachBatch,
    })

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.filter(n => n === 1)).toHaveLength(
      numBatches - numBatchesPerBroker
    )

    await waitForNextEvent(consumer, consumer.events.FETCH_START)
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()

    await consumer.disconnect()
    expect(partitionProcessedBatchCount.every(n => n === 1)).toBeTrue()
    expect(eachBatch).toHaveBeenCalledTimes(numBatches) // numBatches + 1 error
  })
})

describe('Multiple fetch errors are aggregated', () => {
  it('a single processing error is reported as-is', async () => {
    restartOnFailure.mockImplementation(() => false)

    const eachBatch = jest
      .fn(incBatchCountAfter())
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
    expect(restartOnFailure).toHaveBeenCalledWith(noneRetriableError)
  })

  it('multiple processing and fetch request errors are aggregated into a KafkaJSAggregateError', async () => {
    restartOnFailure.mockImplementation(() => false)

    const broker = await cluster.findBroker({ nodeId: 0 })
    const broker0FetchRequest = jest.spyOn(broker, 'fetch')
    const eachBatch = jest.fn(incBatchCountAfter())

    broker0FetchRequest.mockImplementationOnce(throwRetriableErrorAfter(1))
    eachBatch.mockImplementationOnce(throwNoneRetriableErrorAfter(10))
    eachBatch.mockImplementationOnce(throwNoneRetriableErrorAfter(20))

    consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 2,
      eachBatch,
    })

    await Promise.all([
      waitForNextEvent(consumer, consumer.events.CRASH),
      waitForNextEvent(consumer, consumer.events.STOP),
    ])
    expect(restartOnFailure).toHaveBeenCalledWith(
      new KafkaJSAggregateError('Errors while fetching', [
        retriableError,
        noneRetriableError,
        noneRetriableError,
      ])
    )
    expect(restartOnFailure.mock.calls[0][0].retriable).toEqual(false)
  })
})
