const Connection = require('../network/connection')

module.exports = ({
  brokers,
  ssl,
  sasl,
  clientId,
  connectionTimeout,
  maxInFlightRequests,
  retry,
  logger,
}) => {
  const size = brokers.length
  let index = 0

  return {
    build: ({ host, port, rack } = {}) => {
      if (!host) {
        // Always rotate the seed broker
        const [seedHost, seedPort] = brokers[index++ % size].split(':')
        host = seedHost
        port = Number(seedPort)
      }

      return new Connection({
        host,
        port,
        rack,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        maxInFlightRequests,
        retry,
        logger,
      })
    },
  }
}
