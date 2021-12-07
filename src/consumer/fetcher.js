const { EventEmitter } = require('stream')

const fetcher = ({ nodeId, emitter: poolEmitter, fetch }) => {
  const emitter = new EventEmitter()
  emitter.on('batch', () => {
    poolEmitter.emit('batch', { nodeId })
  })
  const queue = []
  let isFetching = false
  let isRunning = true

  const fetchIfNecessary = async () => {
    if (isFetching) return

    if (isRunning) {
      isFetching = true

      const batches = await fetch(nodeId)
      queue.push(...batches)

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
