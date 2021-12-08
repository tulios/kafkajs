const { values } = Object

const fetchManager = ({ logger: rootLogger, nodeIds, fetch, concurrency = 1 }) => {
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

    logger.error('fetchNode()', { nodeId })

    fetchers[nodeId] = (async () => {
      const batches = await fetch(nodeId)
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
