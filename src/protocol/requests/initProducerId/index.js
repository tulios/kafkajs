const versions = {
  0: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  1: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  2: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  3: ({ transactionalId, transactionTimeout = 5000, producerId = -1, producerEpoch = -1 }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return {
      request: request({ transactionalId, transactionTimeout, producerId, producerEpoch }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
