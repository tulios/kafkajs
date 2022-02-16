const seq = require('../utils/seq')
const waitFor = require('../utils/waitFor')
const sleep = require('../utils/sleep')
const createFetcher = require('./fetcher')
const createWorkerQueue = require('./workerQueue')
const createWorker = require('./worker')

describe('Fetcher', () => {
  let fetcher, fetch, handler, nodeIds, workers, workerQueue

  beforeEach(() => {
    fetch = jest.fn(async nodeId => {
      await sleep(50)
      return seq(10, index => `message ${index} from node ${nodeId}`)
    })

    handler = jest.fn(async () => {
      await sleep(50)
    })

    nodeIds = [1, 2, 3]

    workers = seq(5, workerId => createWorker({ workerId, handler }))
    workerQueue = createWorkerQueue({ workers })
    fetcher = createFetcher({ nodeIds, fetch, workerQueue })
  })

  it('should fetch all nodes', async () => {
    fetcher.start()
    await fetcher.stop()

    expect(fetch).toHaveBeenCalledTimes(nodeIds.length)
    nodeIds.forEach(nodeId => {
      expect(fetch).toHaveBeenCalledWith(nodeId)
    })
  })

  it('should utilize all workers', async () => {
    fetcher.start()
    await waitFor(() => handler.mock.calls.length > workers.length)
    await fetcher.stop()

    const calledWorkerIds = handler.mock.calls.map(([, { workerId }]) => workerId)
    workers.forEach(worker => {
      expect(calledWorkerIds).toContain(worker.getWorkerId())
    })
  })
})
