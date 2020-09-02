const Connection = require('../network/connection')
const { KafkaJSNonRetriableError, KafkaJSConnectionError } = require('../errors')
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
  let getNext

  // dynamic list of brokers
  if (typeof brokers === 'function') {
    getNext = async () => {
      try {
        const discovered = await brokers()

        const brokersList = Array.isArray(discovered) ? discovered : discovered.brokers

        const [seedHost, seedPort] = shuffle(brokersList)[0].split(':')

        return {
          host: seedHost,
          port: Number(seedPort),
          sasl: discovered.sasl,
        }
      } catch (e) {
        throw new KafkaJSConnectionError('dynamic brokers function crashed, retrying...')
      }
    }

    // static list of seed brokers
  } else {
    validateBrokers(brokers)

    const shuffledBrokers = shuffle(brokers)
    const size = brokers.length
    let index = 0

    getNext = () => {
      // Always rotate the seed broker
      const [seedHost, seedPort] = shuffledBrokers[index++ % size].split(':')

      return {
        host: seedHost,
        port: Number(seedPort),
        sasl,
      }
    }
  }

  return {
    build: async () => {
      const broker = await getNext()

      return new Connection({
        host: broker.host,
        port: broker.port,
        sasl: broker.sasl,
        ssl,
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
    assign: ({ host, port, rack }) => {
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
