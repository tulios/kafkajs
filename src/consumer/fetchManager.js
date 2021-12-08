const fetchManager = ({ logger: rootLogger, nodeIds, fetch, onCrash }) => {
  const logger = rootLogger.namespace('FetcherPool')
  const fetchers = {}
  const queue = []

  const fetchNode = async nodeId => {
    if (nodeId in fetchers) {
      return fetchers[nodeId]
    }

    logger.debug('fetchNode()', { nodeId })

    fetchers[nodeId] = (async () => {
      const batches = await fetch(nodeId)
      const messages = batches.map(batch => ({ batch, nodeId }))
      queue.push(...messages)
    })()

    try {
      await fetchers[nodeId]
    } catch (error) {
      onCrash(error)
    } finally {
      delete fetchers[nodeId]
    }
  }

  const fetchEmptyNodes = () => {
    const nodesInQueue = new Set(queue.map(({ nodeId }) => nodeId))

    const promises = nodeIds
      .filter(nodeId => !nodesInQueue.has(nodeId))
      .map(nodeId => fetchNode(nodeId))

    return Promise.race(promises)
  }

  const next = async () => {
    // TODO: Support concurrency by assigning topics+partitions to different runners (each to max one runner).
    const fetchPromise = fetchEmptyNodes()

    let message = queue.shift()
    if (!message) {
      await fetchPromise
      message = queue.shift()
    }

    const { batch } = message || {}
    return batch
  }

  return { next }
}

module.exports = fetchManager
