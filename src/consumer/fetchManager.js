const {
  events: { FETCH_START, FETCH },
} = require('./instrumentationEvents')

const { entries } = Object

const fetchManager = ({
  logger: rootLogger,
  instrumentationEmitter,
  nodeIds,
  fetch,
  concurrency = 1,
}) => {
  const logger = rootLogger.namespace('FetchManager')
  const fetchers = {}
  let assignments = {}
  let queues = {}
  let error

  const fetchNode = async (runnerId, nodeId) => {
    if (!(runnerId in fetchers)) fetchers[runnerId] = {}
    if (nodeId in fetchers[runnerId]) return fetchers[nodeId]

    fetchers[runnerId][nodeId] = (async () => {
      logger.error('fetchNode()', { runnerId, nodeId })

      const startFetch = Date.now()
      instrumentationEmitter.emit(FETCH_START, {})

      const batches = await fetch(nodeId, assignments[runnerId])

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
        queues[runnerId].push({ batch, nodeId })
      })
    })()

    try {
      await fetchers[runnerId][nodeId]
    } catch (e) {
      error = e
    } finally {
      delete fetchers[runnerId][nodeId]
    }
  }

  const fetchEmptyNodes = runnerId => {
    const nodesInQueue = new Set(queues[runnerId].map(({ nodeId }) => nodeId))

    const promises = nodeIds
      .filter(nodeId => !nodesInQueue.has(nodeId))
      .map(nodeId => fetchNode(runnerId, nodeId))

    return Promise.race(promises)
  }

  const next = async ({ runnerId }) => {
    logger.error('next()', { runnerId })
    if (error) {
      throw error
    }

    const fetchPromise = fetchEmptyNodes(runnerId)

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

        if (!queues[runnerId]) queues[runnerId] = []

        if (!assignments[runnerId]) assignments[runnerId] = {}
        if (!assignments[runnerId][topic]) assignments[runnerId][topic] = []
        assignments[runnerId][topic].push(partition)
      })

    assignments = entries(assignments).reduce(
      (acc, [runnerId, assignments]) => ({
        ...acc,
        [runnerId]: entries(assignments).map(([topic, partitions]) => ({ topic, partitions })),
      }),
      {}
    )

    logger.error('assigned', { assignments, queues })
  }

  return { next, assign }
}

module.exports = fetchManager
