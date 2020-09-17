const Connection = require('../network/connection')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../errors')

const getBrokers = async brokers => {
  const dynamic = typeof brokers === 'function'

  const list = dynamic ? await brokers() : brokers

  if (!list || list.length === 0) {
    throw dynamic
      ? new KafkaJSConnectionError(`Failed to connect: brokers function returned nothing`)
      : new KafkaJSNonRetriableError(`Failed to connect: expected brokers array and got nothing`)
  }

  return list
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
  let index = 0

  return {
    build: async ({ host, port, rack } = {}) => {
      if (!host) {
        const list = await getBrokers(brokers)

        const randomBroker = list[index++ % list.length]

        host = randomBroker.split(':')[0]
        port = Number(randomBroker.split(':')[1])
      }

      return new Connection({
        host,
        port,
        rack,
        sasl,
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
  }
}
