const Connection = require('../network/connection')

module.exports = ({
  host: seedHost,
  port: seedPort,
  ssl,
  sasl,
  clientId,
  connectionTimeout,
  retry,
  logger,
}) => ({
  build: ({ host, port, rack } = {}) =>
    new Connection({
      host: host || seedHost,
      port: port || seedPort,
      rack,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
      logger,
    }),
})
