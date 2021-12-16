const {
  events: { FETCH_START, FETCH },
} = require('./instrumentationEvents')

const { values } = Object

const fetchManager = ({
  logger: rootLogger,
  instrumentationEmitter,
  nodeIds,
  fetch,
  concurrency = 1,
}) => {
  // eslint-disable-next-line no-unused-vars
  const logger = rootLogger.namespace('FetcherPool')
  const fetchers = {}
  let assignments = {}
  let queues = {}
  let error

  const fetchNode = async nodeId => {
    if (nodeId in fetchers) {
      return fetchers[nodeId]
    }

    fetchers[nodeId] = (async () => {
      logger.error('fetchNode()', { nodeId })

      const startFetch = Date.now()
      instrumentationEmitter.emit(FETCH_START, {})

      const batches = await fetch(nodeId)

      instrumentationEmitter.emit(FETCH, {
        /**
         * PR #570 removed support for the number of batches in this instrumentation event;
         * The new implementation uses an async generation to deliver the batches, which makes
         * this number impossible to get. The number is set to 0 to keep the event backward
         * compatible until we bump KafkaJS to version 2, following the end of node 8 LTS.
         *
         * @since 2019-11-29
         */
        numberOfBatches: 0,
        duration: Date.now() - startFetch,
      })

      batches.forEach(batch => {
        const { topic, partition } = batch
        const runnerId = assignments[topic][partition]
        queues[runnerId].push({ batch, nodeId })
      })
    })()

    try {
      await fetchers[nodeId]
    } catch (e) {
      error = e
    } finally {
      delete fetchers[nodeId]
    }
  }

  const fetchEmptyNodes = () => {
    const nodesInQueues = new Set(
      values(queues)
        .flatMap(x => x)
        .map(({ nodeId }) => nodeId)
    )

    const promises = nodeIds
      .filter(nodeId => !nodesInQueues.has(nodeId))
      .map(nodeId => fetchNode(nodeId))

    return Promise.race(promises)
  }

  const next = async ({ runnerId }) => {
    logger.error('next()', { runnerId })
    if (error) {
      throw error
    }

    const fetchPromise = fetchEmptyNodes()

    const queue = queues[runnerId]

    let message = queue.shift()
    if (!message) {
      await fetchPromise
      message = queue.shift()
    }

    const { batch } = message || {}
    return batch
  }

  const assign = topicPartitions => {
    logger.error('assign()', { topicPartitions })

    assignments = {}
    queues = {}

    topicPartitions
      .flatMap(({ topic, partitions }) => partitions.map(partition => ({ topic, partition })))
      .forEach(({ topic, partition }, index) => {
        const runnerId = index % concurrency

        if (!assignments[topic]) assignments[topic] = {}
        assignments[topic][partition] = runnerId

        if (!queues[runnerId]) queues[runnerId] = []
      })

    logger.error('assigned', { assignments, queues })
  }

  return { next, assign }
}

module.exports = fetchManager
