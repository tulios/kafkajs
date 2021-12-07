const { EventEmitter } = require('stream')

const fetcher = ({ nodeId, emitter: poolEmitter, fetch, logger: rootLogger }) => {
  const logger = rootLogger.namespace('Fetcher')
  const emitter = new EventEmitter()
  const interval = setInterval(() => {}, 1000) // TODO: Hack. Throw away
  const queue = []
  let isFetching = false
  let isRunning = true

  const fetchIfNecessary = async () => {
    logger.debug('fetchIfNecessary()')

    if (isFetching || !isRunning) return
    isFetching = true

    const batches = await fetch(nodeId)
    queue.push(...batches)

    isFetching = false

    poolEmitter.emit('batch', { nodeId })

    if (!isRunning) {
      emitter.emit('finished')
    }
  }

  const stop = async () => {
    logger.debug('stop()')
    isRunning = false

    await new Promise(resolve => {
      if (!isFetching) return resolve()
      emitter.once('finished', () => resolve())
    })

    clearInterval(interval) // TODO: Hack. Throw away
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

  return { stop, next }
}

module.exports = fetcher
