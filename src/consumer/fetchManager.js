const flatten = require('../utils/flatten')
const flatMap = require('../utils/flatMap')
const {
  events: { FETCH_START, FETCH },
} = require('./instrumentationEvents')

const { values } = Object

const fetchManager = ({
  instrumentationEmitter,
  nodeIds,
  fetch,
  concurrency = 1,
  topicPartitions,
}) => {
  const fetchers = {}
  const inProgress = {}
  const queues = Array(concurrency)
    .fill()
    .reduce((acc, _, runnerId) => ({ ...acc, [runnerId]: [] }), {})
  const assignments = {}

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
    const nodesInQueue = new Set(flatten(values(queues)).map(({ nodeId }) => nodeId))

    const promises = nodeIds
      .filter(nodeId => !inProgress[nodeId] && !nodesInQueue.has(nodeId))
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

    let fetchResult = queue.shift()
    if (!fetchResult) {
      await fetchPromise
      fetchResult = queue.shift()
    }

    if (!fetchResult) return callback()
    const { nodeId, batch } = fetchResult

    if (!(nodeId in inProgress)) inProgress[nodeId] = 0

    inProgress[nodeId]++
    try {
      await callback(batch)
    } finally {
      inProgress[nodeId]--
    }
  }

  const getAssignments = () => assignments

  flatMap(topicPartitions, ({ topic, partitions }) =>
    partitions.map(partition => ({ topic, partition }))
  ).forEach(({ topic, partition }, index) => {
    const runnerId = index % concurrency

    if (!assignments[topic]) assignments[topic] = {}
    assignments[topic][partition] = runnerId
  })

  return { next, getAssignments }
}

module.exports = fetchManager
