const Connection = require('../network/connection')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../errors')

/**
 * @typedef {Object} ConnectionBuilder
 * @property {(destination?: { host?: string, port?: number, rack?: string }) => Promise<Connection>} build
 */

/**
 * @param {Object} options
 * @param {import("../../types").ISocketFactory} [options.socketFactory]
 * @param {string[]|(() => string[])} options.brokers
 * @param {Object} [options.ssl]
 * @param {Object} [options.sasl]
 * @param {string} options.clientId
 * @param {number} options.requestTimeout
 * @param {boolean} [options.enforceRequestTimeout]
 * @param {number} [options.connectionTimeout]
 * @param {number} [options.maxInFlightRequests]
 * @param {import("../../types").RetryOptions} [options.retry]
 * @param {import("../../types").Logger} options.logger
 * @param {import("../instrumentation/emitter")} [options.instrumentationEmitter]
 * @returns {ConnectionBuilder}
 */
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

        // awhittier: Works for myserver:9902, 10.1.2.3:9902, and ::1:9902.
        const lastColonIdx = randomBroker.lastIndexOf(':')
        if (lastColonIdx === -1 || lastColonIdx === randomBroker.length - 1) {
          throw new KafkaJSNonRetriableError(
            `Failed to connect: host ${randomBroker} did not contain a host and port separated by a colon`
          )
        }
        host = randomBroker.substring(0, lastColonIdx)
        port = Number(randomBroker.substring(lastColonIdx + 1))
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
        logger,
      })
    },
  }
}
