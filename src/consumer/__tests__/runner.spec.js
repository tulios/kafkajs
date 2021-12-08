const Runner = require('../runner')
const Batch = require('../batch')
const {
  KafkaJSProtocolError,
  KafkaJSNotImplemented,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, secureRandom } = require('testHelpers')
const sleep = require('../../utils/sleep')
const BufferedAsyncIterator = require('../../utils/bufferedAsyncIterator')
const createRunnerPool = require('../runnerPool')

const UNKNOWN = -1
const REBALANCE_IN_PROGRESS = 27
const rebalancingError = () => new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))

describe('Consumer > Runner', () => {
  let runner,
    runnerPool,
    consumerGroup,
    onCrash,
    eachBatch,
    topicName,
    partition,
    emptyBatch,
    instrumentationEmitter

  beforeEach(() => {
    topicName = `topic-${secureRandom()}`
    partition = 0

    emptyBatch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [],
    })

    eachBatch = jest.fn()
    onCrash = jest.fn()
    consumerGroup = {
      connect: jest.fn(),
      join: jest.fn(),
      sync: jest.fn(),
      joinAndSync: jest.fn(),
      leave: jest.fn(),
      fetch: jest.fn(() => BufferedAsyncIterator([Promise.resolve([emptyBatch])])),
      nextBatch: jest.fn(async () => {
        await sleep(50)
      }),
      resolveOffset: jest.fn(),
      commitOffsets: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      uncommittedOffsets: jest.fn(),
      heartbeat: jest.fn(),
      assigned: jest.fn(() => []),
      isLeader: jest.fn(() => true),
    }
    instrumentationEmitter = new InstrumentationEventEmitter()
    runner = new Runner({
      consumerGroup,
      instrumentationEmitter,
      onCrash,
      logger: newLogger(),
      eachBatch,
    })
  })

  afterEach(async () => {
    runner && (await runner.stop())
    runnerPool && (await runnerPool.stop())
  })

  describe('when the group is rebalancing before the new consumer has joined', () => {
    it('recovers from rebalance in progress and re-join the group', async () => {
      consumerGroup.sync
        .mockImplementationOnce(() => {
          throw rebalancingError()
        })
        .mockImplementationOnce(() => {
          throw rebalancingError()
        })
        .mockImplementationOnce(() => true)

      runner.scheduleConsume = jest.fn()
      await runner.start()
      expect(runner.scheduleConsume).toHaveBeenCalled()
      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('should "commit" offsets during fetch', async () => {
    const batch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [{ offset: 4, key: '1', value: '2' }],
    })

    consumerGroup.nextBatch.mockImplementationOnce(async () => batch)
    runner.scheduleConsume = jest.fn()
    await runner.start()
    await runner.consume() // Manually fetch for test
    expect(eachBatch).toHaveBeenCalled()
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(onCrash).not.toHaveBeenCalled()
  })

  describe('"eachBatch" callback', () => {
    it('allows providing offsets to "commitOffsetIfNecessary"', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.nextBatch.mockImplementationOnce(async () => batch)
      runner.scheduleConsume = jest.fn()
      await runner.start()
      await runner.consume() // Manually fetch for test

      expect(eachBatch).toHaveBeenCalledWith(
        expect.objectContaining({
          commitOffsetsIfNecessary: expect.any(Function),
        })
      )

      const { commitOffsetsIfNecessary } = eachBatch.mock.calls[0][0] // Access the callback

      // Clear state
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()

      // No offsets provided
      await commitOffsetsIfNecessary()
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(0)

      // Clear state
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()

      // Provide offsets
      const offsets = {
        topics: [{ topic: topicName, partitions: [{ offset: '1', partition: 0 }] }],
      }

      await commitOffsetsIfNecessary(offsets)
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets)
    })
  })

  describe('when eachBatchAutoResolve is set to false', () => {
    beforeEach(() => {
      runner = new Runner({
        consumerGroup,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        eachBatchAutoResolve: false,
        eachBatch,
        onCrash,
        logger: newLogger(),
      })
      runner.scheduleConsume = jest.fn(() => runner.consume())
    })

    it('does not call resolveOffset with the last offset', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.fetch.mockImplementationOnce(() =>
        BufferedAsyncIterator([Promise.resolve([batch])])
      )
      await runner.start()
      expect(onCrash).not.toHaveBeenCalled()
      expect(consumerGroup.resolveOffset).not.toHaveBeenCalled()
    })
  })

  describe('when autoCommit is set to false', () => {
    let eachBatchCallUncommittedOffsets

    beforeEach(() => {
      eachBatchCallUncommittedOffsets = jest.fn(({ uncommittedOffsets }) => {
        uncommittedOffsets()
      })

      runner = new Runner({
        consumerGroup,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        eachBatch: eachBatchCallUncommittedOffsets,
        onCrash,
        autoCommit: false,
        logger: newLogger(),
      })
      runner.scheduleConsume = jest.fn(() => runner.consume())
    })

    it('should not commit offsets during fetch', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.nextBatch.mockImplementationOnce(async () => batch)
      runner.scheduleConsume = jest.fn()
      await runner.start()
      await runner.consume() // Manually fetch for test

      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eachBatchCallUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.uncommittedOffsets).toHaveBeenCalled()

      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('calls onCrash for any other errors', async () => {
    const unknownError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
    consumerGroup.nextBatch.mockImplementation(() => {
      throw unknownError
    })

    await runner.start()

    // scheduleConsume in runner#start is async, and we never wait for it,
    // so we have to wait a bit to give the callback a chance of being executed
    await sleep(100)

    expect(onCrash).toHaveBeenCalledWith(unknownError)
  })

  it('crashes on KafkaJSNotImplemented errors', async () => {
    const notImplementedError = new KafkaJSNotImplemented('not implemented')
    consumerGroup.nextBatch.mockImplementationOnce(() => Promise.reject(notImplementedError))

    await runner.start()

    // scheduleConsume in runner#start is async, and we never wait for it,
    // so we have to wait a bit to give the callback a chance of being executed
    await sleep(100)

    expect(onCrash).toHaveBeenCalledWith(notImplementedError)
  })

  describe('commitOffsets', () => {
    let offsets

    beforeEach(async () => {
      offsets = { topics: [{ topic: topicName, partitions: [{ offset: '1', partition }] }] }

      consumerGroup.joinAndSync.mockClear()
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()
    })

    it('should commit offsets while running', async () => {
      await runner.start()
      await runner.commitOffsets(offsets)

      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(consumerGroup.commitOffsets.mock.calls.length).toBeGreaterThanOrEqual(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets)
    })

    it('should throw when group is rebalancing, while triggering another join', async () => {
      const error = rebalancingError()
      consumerGroup.commitOffsets.mockImplementationOnce(() => {
        throw error
      })

      await runner.start()

      expect(runner.commitOffsets(offsets)).rejects.toThrow(error.message)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(0)

      await sleep(100)

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })

    it('correctly catch exceptions in parallel "eachBatch" processing', async () => {
      runnerPool = createRunnerPool({
        consumerGroup,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        eachBatchAutoResolve: false,
        eachBatch: async () => {
          throw new Error('Error while processing batches in parallel')
        },
        onCrash,
        logger: newLogger(),
        partitionsConsumedConcurrently: 10,
        retry: { retries: 0 },
      })

      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.nextBatch
        .mockImplementationOnce(async () => {
          await sleep(100)
          return undefined
        })
        .mockImplementationOnce(async () => batch)

      await runnerPool.start()

      await sleep(100)

      await expect(onCrash).toHaveBeenCalledWith(expect.any(KafkaJSNumberOfRetriesExceeded))
    })

    it('correctly catch exceptions in parallel "heartbeat" processing', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.heartbeat = async () => {
        throw new Error('Error while processing heartbeats in parallel')
      }

      consumerGroup.nextBatch
        .mockImplementationOnce(() => sleep(100))
        .mockImplementationOnce(() => batch)

      runner.scheduleConsume = jest.fn()
      await runner.start()

      await expect(runner.consume()).rejects.toThrow(
        'Error while processing heartbeats in parallel'
      )
    })

    it('a triggered rejoin failing should cause a crash', async () => {
      const unknownError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
      consumerGroup.joinAndSync.mockImplementationOnce(() => {
        throw unknownError
      })
      consumerGroup.commitOffsets.mockImplementationOnce(() => {
        throw rebalancingError()
      })

      await runner.start()
      expect(runner.commitOffsets(offsets)).rejects.toThrow(rebalancingError().message)

      await sleep(100)

      expect(onCrash).toHaveBeenCalledWith(unknownError)
    })

    it('should ignore request errors from nextBatch on stopped consumer', async () => {
      consumerGroup.nextBatch
        .mockImplementationOnce(async () => {
          await sleep(10)
          throw new Error('Failed or manually rejected request')
        })
        .mockImplementationOnce(() => sleep(10))

      runner.scheduleConsume = jest.fn()
      await runner.start()
      runner.running = false

      runner.consume()
      await sleep(100)
    })
  })
})
