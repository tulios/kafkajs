const { EventEmitter } = require('stream')
const fetcher = require('./fetcher')

const fetcherPool = ({ logger: rootLogger, nodeIds, fetch }) => {
  const logger = rootLogger.namespace('FetcherPool')
  const emitter = new EventEmitter()
  let isRunning = true

  const fetchers = nodeIds.map(nodeId => fetcher({ nodeId, emitter, fetch, logger }))

  const stop = async () => {
    isRunning = false
    await Promise.all(fetchers.map(f => f.stop()))
  }

  const next = async () => {
    if (!isRunning) {
      return
    }

    for (const fetcher of fetchers) {
      const item = fetcher.next()
      if (item) {
        return item
      }
    }

    return new Promise(resolve => {
      emitter.once('batch', async () => {
        resolve(await next())
      })
    })
  }

  return { stop, next }
}

module.exports = fetcherPool
