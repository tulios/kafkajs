const versions = {
  0: ({ transactionalId, producerId, producerEpoch, transactionResult }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ transactionalId, producerId, producerEpoch, transactionResult }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
