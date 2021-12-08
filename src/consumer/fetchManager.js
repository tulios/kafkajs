const fetchManager = ({ logger: rootLogger, nodeIds, fetch }) => {
  // eslint-disable-next-line no-unused-vars
  const logger = rootLogger.namespace('FetcherPool')
  const fetchers = {}
  let error
  const queue = []

  const fetchNode = async nodeId => {
    if (nodeId in fetchers) {
      return fetchers[nodeId]
    }

    fetchers[nodeId] = (async () => {
      const batches = await fetch(nodeId)
      const messages = batches.map(batch => ({ batch, nodeId }))
      queue.push(...messages)
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
    const nodesInQueue = new Set(queue.map(({ nodeId }) => nodeId))

    const promises = nodeIds
      .filter(nodeId => !nodesInQueue.has(nodeId))
      .map(nodeId => fetchNode(nodeId))

    return Promise.race(promises)
  }

  const next = async () => {
    if (error) {
      throw error
    }

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
