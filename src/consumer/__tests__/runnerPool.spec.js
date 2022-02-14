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
const createRunnerPool = require('../runnerPool')
const waitFor = require('../../utils/waitFor')

const UNKNOWN = -1
const REBALANCE_IN_PROGRESS = 27
const rebalancingError = () => new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))

describe('Consumer > RunnerPool', () => {
  let runnerPool,
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
      nextBatch: jest.fn((_, callback) => callback(emptyBatch)),
      resolveOffset: jest.fn(),
      commitOffsets: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      uncommittedOffsets: jest.fn(),
      heartbeat: jest.fn(),
      assigned: jest.fn(() => []),
      isLeader: jest.fn(() => true),
    }
    instrumentationEmitter = new InstrumentationEventEmitter()
    runnerPool = createRunnerPool({
      consumerGroup,
      instrumentationEmitter,
      onCrash,
      logger: newLogger(),
      eachBatch,
    })
  })

  afterEach(async () => {
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

      await runnerPool.start()
      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('calls onCrash for any other errors', async () => {
    const unknownError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
    consumerGroup.nextBatch.mockImplementation(async () => {
      throw unknownError
    })

    await runnerPool.start()

    await waitFor(() => onCrash.mock.calls.length > 0)
    expect(onCrash).toHaveBeenCalledWith(unknownError)
  })

  it('crashes on KafkaJSNotImplemented errors', async () => {
    const notImplementedError = new KafkaJSNotImplemented('not implemented')
    consumerGroup.nextBatch.mockImplementationOnce(async () => {
      throw notImplementedError
    })

    await runnerPool.start()

    await waitFor(() => onCrash.mock.calls.length > 0)
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
      await runnerPool.start()
      await runnerPool.commitOffsets(offsets)

      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(consumerGroup.commitOffsets.mock.calls.length).toBeGreaterThanOrEqual(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets)
    })

    it('should throw when group is rebalancing, while triggering another join', async () => {
      const error = rebalancingError()
      consumerGroup.commitOffsets.mockImplementationOnce(() => {
        throw error
      })

      await runnerPool.start()

      consumerGroup.joinAndSync.mockClear()

      expect(runnerPool.commitOffsets(offsets)).rejects.toThrow(error.message)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(0)

      await waitFor(() => consumerGroup.joinAndSync.mock.calls.length > 0)
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
        concurrency: 10,
        retry: { retries: 0 },
      })

      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.nextBatch
        .mockImplementationOnce(async (_, callback) => callback(await sleep(100)))
        .mockImplementationOnce(async (_, callback) => callback(batch))

      await runnerPool.start()

      await waitFor(() => onCrash.mock.calls.length > 0)
      await expect(onCrash).toHaveBeenCalledWith(expect.any(KafkaJSNumberOfRetriesExceeded))
    })

    it('a triggered rejoin failing should cause a crash', async () => {
      await runnerPool.start()

      const unknownError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
      consumerGroup.joinAndSync.mockImplementationOnce(() => {
        throw unknownError
      })
      consumerGroup.commitOffsets.mockImplementationOnce(() => {
        throw rebalancingError()
      })

      expect(runnerPool.commitOffsets(offsets)).rejects.toThrow(rebalancingError().message)

      await waitFor(() => onCrash.mock.calls.length > 0)
      expect(onCrash).toHaveBeenCalledWith(unknownError)
    })
  })
})
