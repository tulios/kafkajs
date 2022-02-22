const EventEmitter = require('events')

/**
 * Fetches data from all assigned nodes, waits for workerQueue to drain and repeats.
 *
 * @param {object} options
 * @param {number} options.nodeId
 * @param {import('./workerQueue').WorkerQueue} options.workerQueue
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @template T
 */
const createFetcher = ({ nodeId, workerQueue, fetch }) => {
  const emitter = new EventEmitter()
  let isRunning = false

  const getWorkerQueue = () => workerQueue

  const start = async () => {
    if (isRunning) return
    isRunning = true

    while (isRunning) {
      try {
        const batches = await fetch(nodeId)
        if (isRunning) {
          await workerQueue.push(...batches)
        }
      } catch (error) {
        isRunning = false
        emitter.emit('end')
        throw error
      }
    }
    emitter.emit('end')
  }

  const stop = async () => {
    if (!isRunning) return
    isRunning = false
    await new Promise(resolve => emitter.once('end', () => resolve()))
  }

  return { start, stop, getWorkerQueue }
}

module.exports = createFetcher
