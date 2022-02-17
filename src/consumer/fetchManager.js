const seq = require('../utils/seq')
const createFetcher = require('./fetcher')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')
const { KafkaJSFetcherRebalanceError } = require('../errors')

/** @typedef {ReturnType<typeof createFetchManager>} FetchManager */

/**
 * @param {object} options
 * @param {import('types').Logger} options.logger
 * @param {() => number[]} options.getNodeIds
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @param {import('./worker').Handler<T>} options.handler
 * @param {number} [options.concurrency]
 * @template T
 */
const createFetchManager = ({
  logger: rootLogger,
  getNodeIds,
  fetch,
  handler,
  concurrency = 1,
}) => {
  const logger = rootLogger.namespace('FetchManager')
  let fetchers = []

  const getFetchers = () => fetchers

  const createFetchers = () => {
    const nodeIds = getNodeIds()
    const numFetchers = Math.min(concurrency, nodeIds.length)
    const maxNumWorkers = Math.ceil(concurrency / numFetchers)

    const workers = seq(concurrency, workerId => createWorker({ handler, workerId }))

    const fetchers = seq(numFetchers, index => {
      const fetcherNodeIds = nodeIds.filter((_, i) => i % concurrency === index)
      const fetcherWorkers = workers.slice(maxNumWorkers * index, maxNumWorkers * (index + 1))

      const workerQueue = createWorkerQueue({ workers: fetcherWorkers })

      return createFetcher({
        nodeIds: fetcherNodeIds,
        workerQueue,
        fetch: async nodeId => {
          validateShouldRebalance(nodeIds)
          return fetch(nodeId)
        },
      })
    })

    logger.debug(`Created ${fetchers.length} fetchers`, { nodeIds, concurrency })
    return fetchers
  }

  const validateShouldRebalance = previous => {
    const current = getNodeIds()
    const hasChanged = JSON.stringify(previous.sort()) !== JSON.stringify(current.sort())
    if (hasChanged) {
      throw new KafkaJSFetcherRebalanceError()
    }
  }

  const start = async () => {
    fetchers = createFetchers()

    logger.debug('Starting fetchers...')
    try {
      await Promise.all(fetchers.map(fetcher => fetcher.start()))
    } catch (error) {
      await stop()

      if (error instanceof KafkaJSFetcherRebalanceError) {
        logger.debug('Rebalancing fetchers...')
        return start()
      }

      throw error
    }
  }

  const stop = async () => {
    logger.debug('Stopping fetchers...')
    await Promise.all(fetchers.map(fetcher => fetcher.stop()))
    logger.debug('Stopped fetchers')
  }

  return { start, stop, getFetchers }
}

module.exports = createFetchManager
