const {
  events: { FETCH_START, FETCH },
} = require('./instrumentationEvents')

const { entries } = Object

const fetchManager = ({ instrumentationEmitter, nodeIds, fetch, concurrency = 1 }) => {
  const fetchers = {}
  let assignments = {}
  let queues = {}
  let error

  const fetchNode = async (runnerId, nodeId) => {
    if (!fetchers[runnerId]) fetchers[runnerId] = {}
    if (fetchers[runnerId][nodeId]) return fetchers[runnerId][nodeId]

    fetchers[runnerId][nodeId] = (async () => {
      const startFetch = Date.now()
      instrumentationEmitter.emit(FETCH_START, { nodeId })

      const batches = await fetch(nodeId, assignments[runnerId])

      instrumentationEmitter.emit(FETCH, {
        numberOfBatches: batches.length,
        duration: Date.now() - startFetch,
        nodeId,
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
  }

  return { next, assign }
}

module.exports = fetchManager
