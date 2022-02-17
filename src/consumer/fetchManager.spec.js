const sleep = require('../utils/sleep')
const seq = require('../utils/seq')
const createFetchManager = require('./fetchManager')
const { newLogger } = require('testHelpers')
const waitFor = require('../utils/waitFor')

describe('FetchManager', () => {
  let fetchManager, fetch, handler, getNodeIds, concurrency, batchSize

  const createTestFetchManager = partial =>
    createFetchManager({ logger: newLogger(), concurrency, fetch, handler, getNodeIds, ...partial })

  beforeEach(() => {
    batchSize = 10
    fetch = jest.fn(async nodeId => seq(batchSize, id => `message ${id} from node ${nodeId}`))
    handler = jest.fn(async () => {
      await sleep(20)
    })
    getNodeIds = jest.fn(() => seq(4))
    concurrency = 3
    fetchManager = createTestFetchManager()
  })

  afterEach(async () => {
    fetchManager && (await fetchManager.stop())
  })

  it('should distribute nodeIds evenly', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, getNodeIds: () => seq(2) })
    fetchManager.start()

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)

    const [fetcher1, fetcher2] = fetchers
    expect(fetcher1.getNodeIds()).toEqual([0])
    expect(fetcher2.getNodeIds()).toEqual([1])
  })

  it('should assign nodeIds round-robin', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, getNodeIds: () => seq(5) })
    fetchManager.start()

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)

    const [fetcher1, fetcher2] = fetchers
    expect(fetcher1.getNodeIds()).toEqual([0, 2, 4])
    expect(fetcher2.getNodeIds()).toEqual([1, 3])
  })

  it('should create a single fetcher', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, getNodeIds: () => seq(1) })
    fetchManager.start()

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(1)

    const [fetcher] = fetchers
    expect(fetcher.getNodeIds()).toEqual([0])
    expect(
      fetcher
        .getWorkerQueue()
        .getWorkers()
        .map(x => x.getWorkerId())
    ).toEqual([0, 1])
  })

  it('should finish processing other batches in case of an error from any single worker', async () => {
    handler.mockImplementationOnce(() => {
      throw new Error('test')
    })
    await expect(fetchManager.start()).toReject()
    expect(handler).toHaveBeenCalledTimes((concurrency - 1) * batchSize + 1)
  })

  it('should rebalance fetchers in case of change in nodeIds', async () => {
    getNodeIds.mockImplementation(() => seq(2))

    fetchManager = createTestFetchManager({ concurrency: 3 })
    fetchManager.start()

    let fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)
    expect(fetchers[0].getWorkerQueue().getWorkers()).toHaveLength(2)
    expect(fetchers[1].getWorkerQueue().getWorkers()).toHaveLength(1)

    getNodeIds.mockImplementation(() => seq(3))

    fetch.mockClear()
    await waitFor(() => fetch.mock.calls.length > 0)

    fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(3)
    fetchers.forEach(fetcher => {
      expect(fetcher.getWorkerQueue().getWorkers()).toHaveLength(1)
    })
  })
})
