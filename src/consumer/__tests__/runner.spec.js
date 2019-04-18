const Runner = require('../runner')
const Batch = require('../batch')
const { KafkaJSProtocolError, KafkaJSNotImplemented } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')
const sleep = require('../../utils/sleep')

const UNKNOWN = -1
const REBALANCE_IN_PROGRESS = 27
const rebalancingError = () => new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))

describe('Consumer > Runner', () => {
  let runner, consumerGroup, onCrash, eachBatch

  beforeEach(() => {
    eachBatch = jest.fn()
    onCrash = jest.fn()
    consumerGroup = {
      join: jest.fn(),
      sync: jest.fn(),
      fetch: jest.fn(),
      resolveOffset: jest.fn(),
      commitOffsets: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      uncommittedOffsets: jest.fn(),
      heartbeat: jest.fn(),
      isLeader: jest.fn(() => true),
    }
    const instrumentationEmitter = new InstrumentationEventEmitter()
    runner = new Runner({
      consumerGroup,
      instrumentationEmitter,
      onCrash,
      logger: newLogger(),
      eachBatch,
      partitionsConsumedConcurrently: 1,
    })
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

      runner.scheduleFetch = jest.fn()
      await runner.start()
      expect(runner.scheduleFetch).toHaveBeenCalled()
      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('should "commit" offsets during fetch', async () => {
    const topic = 'topic-name'
    const partition = 0
    const batch = new Batch(topic, 0, {
      partition,
      highWatermark: 5,
      messages: [{ offset: 4, key: '1', value: '2' }],
    })

    consumerGroup.fetch.mockImplementationOnce(() => [batch])
    runner.scheduleFetch = jest.fn()
    await runner.start()
    await runner.fetch() // Manually fetch for test
    expect(eachBatch).toHaveBeenCalled()
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(onCrash).not.toHaveBeenCalled()
  })

  describe('"eachBatch" callback', () => {
    it('allows providing offsets to "commitOffsetIfNecessary"', async () => {
      const topic = 'topic-name'
      const partition = 0
      const batch = new Batch(topic, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.fetch.mockImplementationOnce(() => [batch])
      runner.scheduleFetch = jest.fn()
      await runner.start()
      await runner.fetch() // Manually fetch for test

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
      const offsets = { topics: [{ topic: 'foobar', partitions: [{ offset: '1', partition: 0 }] }] }
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
        partitionsConsumedConcurrently: 1,
      })
      runner.scheduleFetch = jest.fn(() => runner.fetch())
    })

    it('does not call resolveOffset with the last offset', async () => {
      const topic = 'topic-name'
      const partition = 0
      const batch = new Batch(topic, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.fetch.mockImplementationOnce(() => [batch])
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
        partitionsConsumedConcurrently: 1,
      })
      runner.scheduleFetch = jest.fn(() => runner.fetch())
    })

    it('should not commit offsets during fetch', async () => {
      const topic = 'topic-name'
      const partition = 0
      const batch = new Batch(topic, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      consumerGroup.fetch.mockImplementationOnce(() => [batch])
      runner.scheduleFetch = jest.fn()
      await runner.start()
      await runner.fetch() // Manually fetch for test

      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eachBatchCallUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.uncommittedOffsets).toHaveBeenCalled()

      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('calls onCrash for any other errors', async () => {
    const unknowError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
    consumerGroup.join
      .mockImplementationOnce(() => {
        throw unknowError
      })
      .mockImplementationOnce(() => true)

    runner.scheduleFetch = jest.fn()
    await runner.start()

    // scheduleFetch in runner#start is async, and we never wait for it,
    // so we have to wait a bit to give the callback a chance of being executed
    await sleep(100)

    expect(runner.scheduleFetch).not.toHaveBeenCalled()
    expect(onCrash).toHaveBeenCalledWith(unknowError)
  })

  it('crashes on KafkaJSNotImplemented errors', async () => {
    const notImplementedError = new KafkaJSNotImplemented('not implemented')
    consumerGroup.fetch.mockImplementationOnce(() => {
      throw notImplementedError
    })

    await runner.start()

    // scheduleFetch in runner#start is async, and we never wait for it,
    // so we have to wait a bit to give the callback a chance of being executed
    await sleep(100)

    expect(onCrash).toHaveBeenCalledWith(notImplementedError)
  })
})
