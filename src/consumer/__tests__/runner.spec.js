const Runner = require('../runner')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, secureRandom } = require('testHelpers')
const sleep = require('../../utils/sleep')

describe('Consumer > Runner', () => {
  let runner,
    consumerGroup,
    eachBatch,
    topicName,
    partition,
    emptyBatch,
    instrumentationEmitter,
    recover

  beforeEach(() => {
    topicName = `topic-${secureRandom()}`
    partition = 0

    emptyBatch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [],
    })

    eachBatch = jest.fn()
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
    runner = new Runner({
      consumerGroup,
      instrumentationEmitter,
      logger: newLogger(),
      eachBatch,
    })
    recover = jest.fn()
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

    consumerGroup.nextBatch.mockImplementationOnce((_, callback) => callback(batch))
    runner.scheduleConsume = jest.fn()
    await runner.start({ recover })
    await runner.consume() // Manually fetch for test
    expect(eachBatch).toHaveBeenCalled()
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(recover).not.toHaveBeenCalled()
  })

  describe('"eachBatch" callback', () => {
    it('allows providing offsets to "commitOffsetIfNecessary"', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.nextBatch.mockImplementationOnce((_, callback) => callback(batch))
      runner.scheduleConsume = jest.fn()
      await runner.start({ recover })
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

      consumerGroup.nextBatch.mockImplementationOnce((_, callback) => callback(batch))
      await runner.start({ recover })
      expect(recover).not.toHaveBeenCalled()
      expect(consumerGroup.resolveOffset).not.toHaveBeenCalled()
    })
  })

  describe('when autoCommit is set to false', () => {
    let eachBatchCallUncommittedOffsets

    beforeEach(() => {
      eachBatchCallUncommittedOffsets = jest.fn(async ({ uncommittedOffsets }) => {
        uncommittedOffsets()
      })

      runner = new Runner({
        consumerGroup,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        eachBatch: eachBatchCallUncommittedOffsets,
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

      consumerGroup.nextBatch.mockImplementationOnce((_, callback) => callback(batch))
      runner.scheduleConsume = jest.fn()
      await runner.start({ recover })
      await runner.consume() // Manually fetch for test

      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eachBatchCallUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.uncommittedOffsets).toHaveBeenCalled()

      expect(recover).not.toHaveBeenCalled()
    })
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
      .mockImplementationOnce(async (_, callback) => callback(await sleep(100)))
      .mockImplementationOnce(async (_, callback) => callback(batch))

    runner.scheduleConsume = jest.fn()
    await runner.start({ recover })

    await expect(runner.consume()).rejects.toThrow('Error while processing heartbeats in parallel')
  })

  it('should ignore request errors from nextBatch on stopped consumer', async () => {
    consumerGroup.nextBatch
      .mockImplementationOnce(async () => {
        await sleep(10)
        throw new Error('Failed or manually rejected request')
      })
      .mockImplementationOnce(async (_, callback) => callback(await sleep(10)))

    runner.scheduleConsume = jest.fn()
    await runner.start({ recover })
    runner.running = false

    runner.consume()
    await sleep(100)
  })
})
