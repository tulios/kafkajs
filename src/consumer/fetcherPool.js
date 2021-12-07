const sleep = require('../utils/sleep')
const { EventEmitter } = require('stream')
const fetcher = require('./fetcher')

const fetcherPool = ({ logger: rootLogger, nodeIds, fetch, onCrash }) => {
  const logger = rootLogger.namespace('FetcherPool')
  const emitter = new EventEmitter()
  let isRunning = true

  const fetchers = nodeIds.map(nodeId => fetcher({ nodeId, emitter, fetch, logger, onCrash }))

  const stop = async () => {
    isRunning = false
    await Promise.all(fetchers.map(f => f.stop()))
  }

  const next = async () => {
    if (!isRunning) {
      await sleep(50) // TODO: Fix. Infinite loop on runner.stop() otherwise?
      return
    }

    for (const fetcher of fetchers) {
      const item = fetcher.next()
      if (item) {
        return item
      }
    }

    return new Promise(resolve => {
      emitter.once('batch', ({ nodeId }) => {
        const fetcher = fetchers.find(f => f.nodeId === nodeId)
        resolve(fetcher.next())
      })
    })
  }

  return { stop, next }
}

module.exports = fetcherPool
