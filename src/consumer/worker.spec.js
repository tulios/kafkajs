const createWorker = require('./worker')
const Batch = require('./batch')
const seq = require('../utils/seq')
const { newLogger } = require('../../testHelpers')

const createBatch = partition =>
  new Batch('test-topic', 0, {
    partition: partition.toString(),
    highWatermark: '100',
    messages: [
      { offset: '0' },
      { offset: '1' },
      { offset: '2' },
      { offset: '3' },
      { offset: '4' },
      { offset: '5' },
    ],
  })

describe('Worker', () => {
  let worker, handler, next, workerId, partitionAssignments, resolve, reject

  beforeEach(() => {
    resolve = jest.fn(() => {})
    reject = jest.fn(() => {})
    handler = jest.fn(async () => {})
    next = jest.fn(() => undefined)
    workerId = 0
    partitionAssignments = new Map()
    worker = createWorker({ handler, workerId, partitionAssignments, logger: newLogger() })
  })

  it('should loop until next() returns undefined', async () => {
    const [first, second] = seq(2).map(createBatch)
    next
      .mockImplementationOnce(() => ({ batch: first, resolve, reject }))
      .mockImplementationOnce(() => ({ batch: second, resolve, reject }))

    await worker.run({ next })

    expect(next).toHaveBeenCalledTimes(3)
    expect(handler).toHaveBeenCalledTimes(2)
    expect(handler).toHaveBeenCalledWith(first, { workerId })
    expect(handler).toHaveBeenCalledWith(second, { workerId })
    expect(resolve).toHaveBeenCalledTimes(2)
  })

  it('should not catch handler() exceptions', async () => {
    next.mockImplementationOnce(() => ({ batch: 'first', resolve, reject }))

    const error = new Error('test')
    handler.mockImplementationOnce(async () => {
      throw error
    })

    await worker.run({ next })
    expect(reject).toHaveBeenCalledWith(error)
  })

  it('should skip batches that are already assigned to another worker', async () => {
    const secondWorker = createWorker({
      handler,
      workerId: 1,
      partitionAssignments,
      logger: newLogger(),
    })

    const secondNext = jest.fn(() => undefined)
    const batches = seq(2).map(createBatch)

    ;[next, secondNext].forEach(queue => {
      batches.forEach(batch => {
        queue.mockImplementationOnce(() => ({ batch, resolve, reject }))
      })
    })

    await Promise.all([
      worker.run({ next, resolve, reject }),
      secondWorker.run({ next: secondNext, resolve, reject }),
    ])

    expect(next).toHaveBeenCalledTimes(3)
    expect(secondNext).toHaveBeenCalledTimes(3)
    expect(handler).toHaveBeenCalledTimes(2)
  })
})
