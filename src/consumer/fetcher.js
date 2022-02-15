const EventEmitter = require('events')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')

/**
 * @param {object} options
 * @param {number[]} options.nodeIds
 * @param {number[]} options.workerIds
 * @param {import("./worker").Handler<T>} options.handler
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @template T
 */
const createFetcher = ({ nodeIds, workerIds, handler, fetch }) => {
  const workers = workerIds.map(workerId => createWorker({ handler, workerId }))
  const workerQueue = createWorkerQueue({ workers })

  const emitter = new EventEmitter()
  let isRunning = false

  const fetchNodes = async () => {
    const batches = await Promise.all(nodeIds.map(nodeId => fetch(nodeId)))
    return batches.flat()
  }

  const getNodeIds = () => nodeIds
  const getWorkerIds = () => workers.map(x => x.getWorkerId())

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

  return { start, stop, getNodeIds, getWorkerIds }
}

module.exports = createFetcher
