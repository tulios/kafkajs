const { EventEmitter } = require('stream')

const fetcher = ({ nodeId, emitter: poolEmitter, fetch, logger: rootLogger }) => {
  const logger = rootLogger.namespace('Fetcher')
  const emitter = new EventEmitter()
  emitter.on('batch', () => {
    poolEmitter.emit('batch', { nodeId })
  })
  let queue = []
  let isFetching = false
  let isRunning = true

  const fetchIfNecessary = async () => {
    if (isFetching) return

    if (isRunning) {
      isFetching = true

      try {
        const batches = await fetch(nodeId)
        queue.push(...batches)
      } catch (error) {
        logger.error('CRASH', { error })
        throw error // TODO: Handle onCrash across all workers
      }

      isFetching = false
    }

    emitter.emit('batch')
  }

  const stop = async () => {
    isRunning = false

    await new Promise(resolve => {
      if (!isFetching) return resolve()
      emitter.once('batch', () => resolve())
    })

    queue = []
  }

  const next = () => {
    const item = queue.shift()
    if (!queue.length) {
      fetchIfNecessary()
    }
    return item
  }

  fetchIfNecessary()

  return { nodeId, stop, next }
}

module.exports = fetcher
