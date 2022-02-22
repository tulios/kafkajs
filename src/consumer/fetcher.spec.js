const seq = require('../utils/seq')
const waitFor = require('../utils/waitFor')
const sleep = require('../utils/sleep')
const createFetcher = require('./fetcher')
const createWorkerQueue = require('./workerQueue')
const createWorker = require('./worker')

describe('Fetcher', () => {
  let fetcher, fetch, handler, workerIds, workers, workerQueue

  beforeEach(() => {
    fetch = jest.fn(async nodeId => {
      await sleep(50)
      return seq(10, index => `message ${index} from node ${nodeId}`)
    })

    handler = jest.fn(async () => {
      await sleep(50)
    })

    workerIds = seq(5)
    workers = workerIds.map(workerId => createWorker({ workerId, handler }))
    workerQueue = createWorkerQueue({ workers })
    fetcher = createFetcher({ nodeId: 0, fetch, workerQueue })
  })

  it('should fetch, but not push to workerQueue before exiting', async () => {
    fetcher.start()
    await fetcher.stop()

    expect(fetch).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledWith(0)
    expect(handler).toHaveBeenCalledTimes(0)
  })

  it('should utilize all workers', async () => {
    fetcher.start()

    await waitFor(() => handler.mock.calls.length > workerIds.length)
    await fetcher.stop()

    const calledWorkerIds = handler.mock.calls.map(([, { workerId }]) => workerId)
    workerIds.forEach(workerId => {
      expect(calledWorkerIds).toContain(workerId)
    })
  })
})
