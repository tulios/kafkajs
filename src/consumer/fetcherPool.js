const { EventEmitter } = require('stream')
const fetcher = require('./fetcher')

const fetcherPool = ({ nodeIds = [], fetch = () => {} } = {}) => {
  const emitter = new EventEmitter()

  const fetchers = nodeIds.map(nodeId => fetcher({ nodeId, emitter, fetch }))

  const stop = () => {
    fetchers.forEach(f => f.stop())
  }

  const next = async () => {
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
