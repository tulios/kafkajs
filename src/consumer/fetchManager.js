const seq = require('../utils/seq')
const createFetcher = require('./fetcher')

/**
 * @param {object} options
 * @param {number[]} options.nodeIds
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @param {(batch: T) => Promise<void>} options.handler
 * @param {number} [options.concurrency]
 * @template T
 */
const createFetchManager = ({ nodeIds, fetch, handler, concurrency = 1 }) => {
  const numFetchers = Math.min(concurrency, nodeIds.length)
  const maxNumWorkers = Math.ceil(concurrency / numFetchers)

  const workerIds = seq(concurrency)

  const fetchers = seq(numFetchers, index => {
    const fetcherNodeIds = nodeIds.filter((_, i) => i % concurrency === index)
    const fetcherWorkerIds = workerIds.slice(
      index * maxNumWorkers,
      index * maxNumWorkers + maxNumWorkers
    )

    return createFetcher({ nodeIds: fetcherNodeIds, workerIds: fetcherWorkerIds, fetch, handler })
  })

  const getFetchers = () => fetchers

  const start = async () => {
    try {
      await Promise.all(fetchers.map(fetcher => fetcher.start()))
    } catch (error) {
      await Promise.all(fetchers.map(fetcher => fetcher.stop()))
      throw error
    }
  }

  return { start, getFetchers }
}

module.exports = createFetchManager
