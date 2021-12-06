const fetcher = ({ nodeId, emitter, fetch }) => {
  const queue = []
  let isFetching = false
  let isRunning = true

  const fetchIfNecessary = async () => {
    if (isFetching) return
    isFetching = true

    if (isRunning) {
      const batches = await fetch()
      queue.push(...batches)
    }

    isFetching = false

    emitter.emit('batch', { nodeId })
  }

  const stop = () => {
    isRunning = false
  }

  const next = () => {
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
