const Runner = require('../runner')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, secureRandom, waitFor } = require('testHelpers')
const sleep = require('../../utils/sleep')

describe('Consumer > Runner', () => {
  let runner,
    commitOffsetsIfNecessary,
    commitOffsets,
    hasSeekOffset,
    heartbeat,
    nextBatch,
    resolveOffset,
    uncommittedOffsets,
    instrumentationEmitter,
    eachBatch,
    topicName,
    partition,
    emptyBatch,
    recover

  /** @param {Partial<ConstructorParameters<typeof Runner>[0]>} [partial]  */
  const createTestRunner = partial => {
    return new Runner({
      commitOffsetsIfNecessary,
      commitOffsets,
      hasSeekOffset,
      heartbeat,
      nextBatch,
      resolveOffset,
      uncommittedOffsets,
      instrumentationEmitter,
      logger: newLogger(),
      eachBatch,
      ...partial,
    })
  }

  beforeEach(() => {
    topicName = `topic-${secureRandom()}`
    partition = 0

    emptyBatch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [],
    })

    commitOffsetsIfNecessary = jest.fn()
    commitOffsets = jest.fn()
    hasSeekOffset = jest.fn(() => false)
    heartbeat = jest.fn()
    nextBatch = jest.fn(callback => callback(emptyBatch))
    resolveOffset = jest.fn()
    uncommittedOffsets = jest.fn()
    instrumentationEmitter = new InstrumentationEventEmitter()
    eachBatch = jest.fn()
    recover = jest.fn()

    runner = createTestRunner()
  })

  afterEach(async () => {
    runner && (await runner.stop())
  })

  it('should "commit" offsets during fetch', async () => {
    const batch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [{ offset: 4, key: '1', value: '2' }],
    })

    nextBatch.mockImplementationOnce(callback => callback(batch))
    runner.scheduleConsume = jest.fn()
    await runner.start(recover)
    await runner.consume() // Manually fetch for test
    expect(eachBatch).toHaveBeenCalled()
    expect(commitOffsets).toHaveBeenCalled()
    expect(recover).not.toHaveBeenCalled()
  })

  describe('"eachBatch" callback', () => {
    it('allows providing offsets to "commitOffsetIfNecessary"', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      nextBatch.mockImplementationOnce(callback => callback(batch))
      runner.scheduleConsume = jest.fn()
      await runner.start(recover)
      await runner.consume() // Manually fetch for test

      expect(eachBatch).toHaveBeenCalledWith(
        expect.objectContaining({
          commitOffsetsIfNecessary: expect.any(Function),
        })
      )

      const {
        commitOffsetsIfNecessary: eachBatchCommitOffsetsIfNecessary,
      } = eachBatch.mock.calls[0][0] // Access the callback

      // Clear state
      commitOffsetsIfNecessary.mockClear()
      commitOffsets.mockClear()

      // No offsets provided
      await eachBatchCommitOffsetsIfNecessary()
      expect(commitOffsetsIfNecessary).toHaveBeenCalledTimes(1)
      expect(commitOffsets).toHaveBeenCalledTimes(0)

      // Clear state
      commitOffsetsIfNecessary.mockClear()
      commitOffsets.mockClear()

      // Provide offsets
      const offsets = {
        topics: [{ topic: topicName, partitions: [{ offset: '1', partition: 0 }] }],
      }

      await eachBatchCommitOffsetsIfNecessary(offsets)
      expect(commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(commitOffsets).toHaveBeenCalledTimes(1)
      expect(commitOffsets).toHaveBeenCalledWith(offsets)
    })
  })

  describe('when eachBatchAutoResolve is set to false', () => {
    beforeEach(() => {
      runner = createTestRunner({ eachBatchAutoResolve: false })
      runner.scheduleConsume = jest.fn(() => runner.consume())
    })

    it('does not call resolveOffset with the last offset', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      nextBatch.mockImplementationOnce(callback => callback(batch))
      await runner.start(recover)
      expect(recover).not.toHaveBeenCalled()
      expect(resolveOffset).not.toHaveBeenCalled()
    })
  })

  describe('when autoCommit is set to false', () => {
    let eachBatchCallUncommittedOffsets

    beforeEach(() => {
      eachBatchCallUncommittedOffsets = jest.fn(async ({ uncommittedOffsets }) => {
        uncommittedOffsets()
      })

      runner = createTestRunner({ autoCommit: false, eachBatch: eachBatchCallUncommittedOffsets })
      runner.scheduleConsume = jest.fn(() => runner.consume())
    })

    it('should not commit offsets during fetch', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      nextBatch.mockImplementationOnce(callback => callback(batch))
      runner.scheduleConsume = jest.fn()
      await runner.start(recover)
      await runner.consume() // Manually fetch for test

      expect(commitOffsets).not.toHaveBeenCalled()
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eachBatchCallUncommittedOffsets).toHaveBeenCalled()
      expect(uncommittedOffsets).toHaveBeenCalled()

      expect(recover).not.toHaveBeenCalled()
    })
  })

  it('correctly catch exceptions in parallel "heartbeat" processing', async () => {
    const batch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [{ offset: 4, key: '1', value: '2' }],
    })

    nextBatch.mockImplementation(async callback => {
      await sleep(100)
      return callback(batch)
    })

    const error = new Error('Error while processing heartbeats in parallel')
    heartbeat.mockImplementation(async () => {
      throw error
    })

    await runner.start(recover)

    await waitFor(() => recover.mock.calls.length > 0)
    expect(recover).toHaveBeenCalledWith(error)
  })

  it('should ignore request errors from nextBatch on stopped consumer', async () => {
    nextBatch
      .mockImplementationOnce(async () => {
        await sleep(10)
        throw new Error('Failed or manually rejected request')
      })
      .mockImplementationOnce(async callback => callback(await sleep(10)))

    runner.scheduleConsume = jest.fn()
    await runner.start(recover)
    runner.running = false

    runner.consume()
    await sleep(100)
  })
})
