const EventEmitter = require('events')

/**
 * Fetches data from all assigned nodes, waits for workerQueue to drain and repeats.
 *
 * @param {object} options
 * @param {number[]} options.nodeIds
 * @param {import('./workerQueue').WorkerQueue} options.workerQueue
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @template T
 */
const createFetcher = ({ nodeIds, workerQueue, fetch }) => {
  const emitter = new EventEmitter()
  let isRunning = false

  const getNodeIds = () => nodeIds

  const getWorkerQueue = () => workerQueue

  const fetchNodes = async () => {
    const batches = await Promise.all(nodeIds.map(nodeId => fetch(nodeId)))
    return batches.flat()
  }

  const start = async () => {
    isRunning = true

    try {
      const batches = await fetchNodes()

      if (isRunning) {
        await workerQueue.push(...batches)
      }
    } catch (error) {
      isRunning = false
      emitter.emit('end')
      throw error
    }

    if (isRunning) {
      return start()
    }
    emitter.emit('end')
  }

  const stop = async () => {
    if (!isRunning) {
      return
    }
    isRunning = false
    await new Promise(resolve => emitter.once('end', () => resolve()))
  }

  return { getNodeIds, getWorkerQueue, start, stop }
}

module.exports = createFetcher
