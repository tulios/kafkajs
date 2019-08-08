const Connection = require('../network/connection')
const { KafkaJSNonRetriableError } = require('../errors')

module.exports = ({
  socketFactory,
  brokers,
  ssl,
  sasl,
  clientId,
  requestTimeout,
  enforceRequestTimeout,
  connectionTimeout,
  maxInFlightRequests,
  retry,
  logger,
  instrumentationEmitter = null,
}) => {
  const size = brokers.length
  let index = 0

  if (size === 0) {
    throw new KafkaJSNonRetriableError('Brokers list cannot be empty')
  }

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
        socketFactory,
        connectionTimeout,
        requestTimeout,
        enforceRequestTimeout,
        maxInFlightRequests,
        instrumentationEmitter,
        retry,
        logger,
      })
    },
  }
}
