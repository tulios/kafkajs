const Connection = require('../network/connection')
const { KafkaJSNonRetriableError } = require('../errors')
const shuffle = require('../utils/shuffle')

const validateBrokers = brokers => {
  if (!brokers || brokers.length === 0) {
    throw new KafkaJSNonRetriableError(`Failed to connect: expected brokers array and got nothing`)
  }
}

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
  validateBrokers(brokers)

  const shuffledBrokers = shuffle(brokers)
  const size = brokers.length
  let index = 0

  return {
    build: ({ host, port, rack } = {}) => {
      if (!host) {
        // Always rotate the seed broker
        const [seedHost, seedPort] = shuffledBrokers[index++ % size].split(':')
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
