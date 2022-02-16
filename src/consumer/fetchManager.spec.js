const sleep = require('../utils/sleep')
const seq = require('../utils/seq')
const createFetchManager = require('./fetchManager')

describe('FetchManager', () => {
  let fetchManager, fetch, handler, nodeIds, concurrency, batchSize

  const createTestFetchManager = partial =>
    createFetchManager({ concurrency, fetch, handler, nodeIds, ...partial })

  beforeEach(() => {
    batchSize = 10
    fetch = jest.fn(async nodeId => seq(batchSize, id => `message ${id} from node ${nodeId}`))
    handler = jest.fn(async () => {
      await sleep(20)
    })
    nodeIds = seq(4)
    concurrency = 3
    fetchManager = createTestFetchManager()
  })

  it('should distribute nodeIds evenly', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, nodeIds: seq(2) })

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)

    const [fetcher1, fetcher2] = fetchers
    expect(fetcher1.getNodeIds()).toEqual([0])
    expect(fetcher2.getNodeIds()).toEqual([1])
  })

  it('should assign nodeIds round-robin', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, nodeIds: seq(5) })

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)

    const [fetcher1, fetcher2] = fetchers
    expect(fetcher1.getNodeIds()).toEqual([0, 2, 4])
    expect(fetcher2.getNodeIds()).toEqual([1, 3])
  })

  it('should create a single fetcher', async () => {
    fetchManager = createTestFetchManager({ concurrency: 2, nodeIds: seq(1) })

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
})
