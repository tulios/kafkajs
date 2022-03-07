const seq = require('../utils/seq')
const createFetcher = require('./fetcher')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')
const { KafkaJSFetcherRebalanceError } = require('../errors')

/** @typedef {ReturnType<typeof createFetchManager>} FetchManager */

/**
 * @param {object} options
 * @param {import('../../types').Logger} options.logger
 * @param {() => number[]} options.getNodeIds
 * @param {(nodeId: number) => Promise<import('../../types').Batch[]>} options.fetch
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
  const partitionAssignments = new Map()
  const workers = seq(concurrency, workerId =>
    createWorker({ handler, workerId, partitionAssignments, logger })
  )
  const workerQueue = createWorkerQueue({ workers })

  let fetchers = []

  const getFetchers = () => fetchers

  const createFetchers = () => {
    const nodeIds = getNodeIds()

    const validateShouldRebalance = () => {
      const current = getNodeIds()
      const hasChanged =
        nodeIds.length !== current.length || nodeIds.some(nodeId => !current.includes(nodeId))
      if (hasChanged) {
        throw new KafkaJSFetcherRebalanceError()
      }
    }

    const fetchers = nodeIds.map(nodeId =>
      createFetcher({
        nodeId,
        workerQueue,
        fetch: async nodeId => {
          validateShouldRebalance()
          return await fetch(nodeId)
        },
      })
    )

    logger.debug(`Created ${fetchers.length} fetchers`, { nodeIds, concurrency })
    return fetchers
  }

  const start = async () => {
    logger.debug('Starting...')

    while (true) {
      fetchers = createFetchers()

      try {
        await Promise.all(fetchers.map(fetcher => fetcher.start()))
      } catch (error) {
        await stop()

        if (error instanceof KafkaJSFetcherRebalanceError) {
          logger.debug('Rebalancing fetchers...')
          continue
        }

        throw error
      }

      break
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
