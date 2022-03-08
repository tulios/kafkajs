const seq = require('../utils/seq')
const waitFor = require('../utils/waitFor')
const sleep = require('../utils/sleep')
const createFetcher = require('./fetcher')
const createWorkerQueue = require('./workerQueue')
const createWorker = require('./worker')
const { newLogger } = require('../../testHelpers')
const Batch = require('./batch')
const { describe, afterEach } = require('jest-circus')

describe('Fetcher', () => {
  let fetcher, fetch, handler, workerIds, workers, workerQueue, logger, fetcherAssignments

  beforeEach(() => {
    fetch = jest.fn(async nodeId => {
      await sleep(1)
      return seq(
        10,
        index =>
          new Batch('test-topic', 0, {
            partition: `${nodeId}${index}`,
            highWatermark: '100',
            messages: [],
          })
      )
    })

    handler = jest.fn(async () => {
      await sleep(1)
    })

    logger = newLogger()
    workerIds = seq(5)
    const workerAssignments = new Map()
    workers = workerIds.map(workerId =>
      createWorker({ workerId, handler, partitionAssignments: workerAssignments, logger })
    )
    workerQueue = createWorkerQueue({ workers })
    fetcherAssignments = new Map()
    fetcher = createFetcher({
      nodeId: 0,
      fetch,
      workerQueue,
      logger,
      partitionAssignments: fetcherAssignments,
    })
  })

  afterEach(async () => {
    fetcher && (await fetcher.stop())
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

  describe('handles partition reassignment', () => {
    let fetchers

    afterEach(async () => {
      fetchers && (await Promise.all(fetchers.map(f => f.stop())))
    })

    it('should filter out batches currently processed by another fetcher', async () => {
      const batches = seq(2).map(
        index =>
          new Batch('test-topic', 0, {
            partition: index.toString(),
            highWatermark: '100',
            messages: [],
          })
      )
      const fetch = jest.fn(async _ => {
        await sleep(1)
        const batch = batches.pop()

        if (!batch) {
          return []
        }

        const reassignedPartition = new Batch('test-topic', 0, {
          partition: 'reassigned',
          highWatermark: '100',
          messages: [],
        })
        return [reassignedPartition, batch]
      })
      const fetchers = seq(2).map(index =>
        createFetcher({
          nodeId: index,
          fetch,
          workerQueue,
          logger,
          partitionAssignments: fetcherAssignments,
        })
      )

      fetchers.forEach(fetcher => fetcher.start())

      await waitFor(() => handler.mock.calls.length >= 3)
      await Promise.all(fetchers.map(fetcher => fetcher.stop()))

      expect(handler).toHaveBeenCalledTimes(3)
    })
  })
})
