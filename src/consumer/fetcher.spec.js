const seq = require('../utils/seq')
const waitFor = require('../utils/waitFor')
const sleep = require('../utils/sleep')
const createFetcher = require('./fetcher')

describe('Fetcher', () => {
  let fetcher, fetch, handler, nodeIds, workerIds

  beforeEach(() => {
    fetch = jest.fn(async nodeId => {
      await sleep(50)
      return seq(10, index => `message ${index} from node ${nodeId}`)
    })

    handler = jest.fn(async () => {
      await sleep(50)
    })

    nodeIds = [1, 2, 3]
    workerIds = seq(5)

    fetcher = createFetcher({ fetch, handler, nodeIds, workerIds })
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
    await waitFor(() => handler.mock.calls.length > workerIds.length)
    await fetcher.stop()

    const calledWorkerIds = handler.mock.calls.map(([, { workerId }]) => workerId)
    workerIds.forEach(workerId => {
      expect(calledWorkerIds).toContain(workerId)
    })
  })
})
