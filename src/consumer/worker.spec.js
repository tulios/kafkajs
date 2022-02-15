const createWorker = require('./worker')

describe('Worker', () => {
  let worker, handler, next, workerId

  beforeEach(() => {
    handler = jest.fn()
    next = jest.fn(() => 'test')
    workerId = 0
    worker = createWorker({ handler, workerId })
  })

  it('should loop until next() returns undefined', async () => {
    next
      .mockImplementationOnce(() => 'first')
      .mockImplementationOnce(() => 'second')
      .mockImplementationOnce(() => undefined)

    await worker.run({ next })

    expect(next).toHaveBeenCalledTimes(3)
    expect(handler).toHaveBeenCalledTimes(2)
    expect(handler).toHaveBeenCalledWith('first', { workerId })
    expect(handler).toHaveBeenCalledWith('second', { workerId })
  })

  it('should not catch next() exceptions', async () => {
    next.mockImplementationOnce(() => {
      throw new Error('test')
    })
    await expect(worker.run({ next })).toReject()
  })

  it('should not catch handler() exceptions', async () => {
    handler.mockImplementationOnce(async () => {
      throw new Error('test')
    })
    await expect(worker.run({ next })).toReject()
  })
})
