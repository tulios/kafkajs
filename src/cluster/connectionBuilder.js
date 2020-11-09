const Connection = require('../network/connection')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../errors')

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

  const getBrokers = async () => {
    if (!brokers) {
      throw new KafkaJSNonRetriableError(`Failed to connect: brokers parameter should not be null`)
    }

    // static list
    if (Array.isArray(brokers)) {
      if (!brokers.length) {
        throw new KafkaJSNonRetriableError(`Failed to connect: brokers array is empty`)
      }
      return brokers
    }

    // dynamic brokers
    let list
    try {
      list = await brokers()
    } catch (e) {
      const wrappedError = new KafkaJSConnectionError(
        `Failed to connect: "config.brokers" threw: ${e.message}`
      )
      wrappedError.stack = `${wrappedError.name}\n  Caused by: ${e.stack}`
      throw wrappedError
    }

    if (!list || list.length === 0) {
      throw new KafkaJSConnectionError(
        `Failed to connect: "config.brokers" returned void or empty array`
      )
    }
    return list
  }

  return {
    build: async ({ host, port, rack } = {}) => {
      if (!host) {
        const list = await getBrokers()

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
