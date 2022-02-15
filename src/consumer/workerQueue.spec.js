const createWorkerQueue = require('./workerQueue')
const createWorker = require('./worker')
const seq = require('../utils/seq')

describe('WorkerQueue', () => {
  const batches = seq(100, index => `message ${index}`)
  let workerQueue, workers, handler

  beforeEach(() => {
    handler = jest.fn(() => {})

    workers = seq(3, workerId => createWorker({ handler, workerId }))
    workerQueue = createWorkerQueue({ workers })
  })

  it('should handle all messages within on push', async () => {
    await workerQueue.push(...batches)
    expect(handler).toHaveBeenCalledTimes(100)
  })

  it('should should finish processing before throwing exception', async () => {
    handler.mockImplementationOnce(() => {
      throw new Error('test')
    })
    await expect(workerQueue.push(...batches)).toReject()
    expect(handler).toHaveBeenCalledTimes(100)
  })
})
