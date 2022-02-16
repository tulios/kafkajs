const seq = require('../utils/seq')
const createFetcher = require('./fetcher')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')

/**
 * @param {object} options
 * @param {import('types').Logger} options.logger
 * @param {number[]} options.nodeIds
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @param {import('./worker').Handler<T>} options.handler
 * @param {number} [options.concurrency]
 * @template T
 */
const createFetchManager = ({ nodeIds, fetch, handler, concurrency = 1 }) => {
  const initFetchers = () => {
    const numFetchers = Math.min(concurrency, nodeIds.length)
    const maxNumWorkers = Math.ceil(concurrency / numFetchers)

    const workerIds = seq(concurrency)

    return seq(numFetchers, index => {
      const fetcherNodeIds = nodeIds.filter((_, i) => i % concurrency === index)
      const fetcherWorkerIds = workerIds.slice(
        index * maxNumWorkers,
        index * maxNumWorkers + maxNumWorkers
      )

      const workers = fetcherWorkerIds.map(workerId => createWorker({ handler, workerId }))
      const workerQueue = createWorkerQueue({ workers })
      return createFetcher({ nodeIds: fetcherNodeIds, workerQueue, fetch })
    })
  }

  const fetchers = initFetchers()

  const getFetchers = () => fetchers

  const start = async () => {
    try {
      await Promise.all(fetchers.map(fetcher => fetcher.start()))
    } catch (error) {
      await stop()
      throw error
    }
  }

  const stop = async () => {
    await Promise.all(fetchers.map(fetcher => fetcher.stop()))
  }

  return { start, stop, getFetchers }
}

module.exports = createFetchManager
