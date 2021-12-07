const { EventEmitter } = require('stream')

const fetcher = ({ nodeId, emitter: poolEmitter, fetch, logger: rootLogger, onCrash }) => {
  // eslint-disable-next-line no-unused-vars
  const logger = rootLogger.namespace('Fetcher')
  const emitter = new EventEmitter()
  emitter.on('batch', () => {
    poolEmitter.emit('batch', { nodeId })
  })
  let queue = []
  let isFetching = false
  let isRunning = true

  const fetchIfNecessary = async () => {
    logger.debug('fetchIfNecessary()')

    if (isFetching) return

    let error

    if (isRunning) {
      isFetching = true

      try {
        const batches = await fetch(nodeId)
        queue.push(...batches)
      } catch (e) {
        error = e
      }

      isFetching = false
    }

    emitter.emit('batch')

    if (error) {
      await stop()
      onCrash(error)
    }
  }

  const stop = async () => {
    logger.debug('stop()')

    isRunning = false

    await new Promise(resolve => {
      if (!isFetching) return resolve()
      emitter.once('batch', () => resolve())
    })

    queue = []
  }

  const next = () => {
    logger.debug('next()')

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
