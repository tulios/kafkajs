const sleep = require('../utils/sleep')
const seq = require('../utils/seq')
const createFetchManager = require('./fetchManager')
const Batch = require('./batch')
const { newLogger } = require('testHelpers')
const waitFor = require('../utils/waitFor')
const { KafkaJSNonRetriableError, KafkaJSNoBrokerAvailableError } = require('../errors')

describe('FetchManager', () => {
  let fetchManager, fetch, handler, getNodeIds, concurrency, batchSize

  const createTestFetchManager = partial =>
    createFetchManager({ logger: newLogger(), concurrency, fetch, handler, getNodeIds, ...partial })

  beforeEach(() => {
    batchSize = 10
    fetch = jest.fn(async nodeId =>
      seq(
        batchSize,
        id =>
          new Batch('test-topic', 0, {
            partition: `${nodeId}${id}`,
            highWatermark: '100',
            messages: [],
          })
      )
    )
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

  it('should construct fetchers and workers', async () => {
    fetchManager.start()

    const fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(getNodeIds().length)

    const workerQueue = fetchers[0].getWorkerQueue()
    const workers = workerQueue.getWorkers()
    expect(workers).toHaveLength(concurrency)
  })

  it('should finish processing other batches in case of an error from any single worker', async () => {
    handler.mockImplementationOnce(() => {
      throw new Error('test')
    })
    await expect(fetchManager.start()).toReject()
    expect(handler).toHaveBeenCalledTimes(getNodeIds().length * batchSize)
  })

  it('should rebalance fetchers in case of change in nodeIds', async () => {
    getNodeIds.mockImplementation(() => seq(2))

    fetchManager = createTestFetchManager({ concurrency: 3 })
    fetchManager.start()

    let fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(2)

    getNodeIds.mockImplementation(() => seq(3))

    fetch.mockClear()
    await waitFor(() => fetch.mock.calls.length > 0)

    fetchers = fetchManager.getFetchers()
    expect(fetchers).toHaveLength(3)
  })

  describe('when all brokers have become unavailable', () => {
    it('should not rebalance and let the error bubble up', async () => {
      const fetchMock = jest.fn().mockImplementation(async nodeId => {
        if (!getNodeIds().includes(nodeId)) {
          throw new KafkaJSNonRetriableError('Node not found')
        }

        return fetch(nodeId)
      })
      getNodeIds.mockImplementation(() => seq(1))

      fetchManager = createTestFetchManager({ concurrency: 1, fetch: fetchMock })
      const fetchManagerPromise = fetchManager.start()

      expect(fetchManager.getFetchers()).toHaveLength(1)

      getNodeIds.mockImplementation(() => seq(0))
      await expect(fetchManagerPromise).rejects.toThrow('Node not found')
    })
  })

  it('should throw an error when there are no brokers available', async () => {
    getNodeIds.mockImplementation(() => seq(0))

    await expect(fetchManager.start()).rejects.toThrowError(new KafkaJSNoBrokerAvailableError())
  })
})
