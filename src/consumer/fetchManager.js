const {
  events: { FETCH_START, FETCH },
} = require('./instrumentationEvents')

const { values } = Object

const fetchManager = ({ instrumentationEmitter, nodeIds, fetch, concurrency = 1 }) => {
  const fetchers = {}
  const inflight = {}
  const queues = Array(concurrency)
    .fill()
    .reduce((acc, _, runnerId) => ({ ...acc, [runnerId]: [] }), {})
  let assignments = {}
  let error

  const fetchNode = async nodeId => {
    if (fetchers[nodeId]) return fetchers[nodeId]

    fetchers[nodeId] = (async () => {
      const startFetch = Date.now()
      instrumentationEmitter.emit(FETCH_START, { nodeId })

      const batches = await fetch(nodeId)

      instrumentationEmitter.emit(FETCH, {
        numberOfBatches: batches.length,
        duration: Date.now() - startFetch,
        nodeId,
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
    const nodesInQueue = new Set(
      values(queues)
        .flatMap(x => x)
        .map(({ nodeId }) => nodeId)
    )

    const promises = nodeIds
      .filter(nodeId => !inflight[nodeId] && !nodesInQueue.has(nodeId))
      .map(nodeId => fetchNode(nodeId))

    if (promises.length) {
      return Promise.race(promises)
    }
  }

  const next = async ({ runnerId, callback }) => {
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

    if (!message) return callback()
    const { nodeId, batch } = message

    if (!(nodeId in inflight)) inflight[nodeId] = 0

    inflight[nodeId]++
    try {
      await callback(batch)
    } finally {
      inflight[nodeId]--
    }
  }

  const assign = topicPartitions => {
    assignments = {}

    topicPartitions
      .flatMap(({ topic, partitions }) => partitions.map(partition => ({ topic, partition })))
      .forEach(({ topic, partition }, index) => {
        const runnerId = index % concurrency

        if (!assignments[topic]) assignments[topic] = {}
        assignments[topic][partition] = runnerId
      })
  }

  return { next, assign }
}

module.exports = fetchManager
