const BrokerPool = require('./brokerPool')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const { KafkaJSNonRetriableError } = require('../errors')

const { values, keys } = Object
const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `connectionPool.events.${key}`)
  .join(', ')

module.exports = class ConnectionPool extends BrokerPool {
  /**
   * @param {object} options
   * @param {import("./connectionBuilder").ConnectionBuilder} options.connectionBuilder
   * @param {import("../../types").Logger} options.logger
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {boolean} [options.allowAutoTopicCreation]
   * @param {number} [options.authenticationTimeout]
   * @param {number} [options.reauthenticationThreshold]
   * @param {number} [options.metadataMaxAge]
   * @param {InstrumentationEventEmitter} [options.instrumentationEmitter]
   */
  constructor({
    instrumentationEmitter: rootInstrumentationEmitter,
    logger: rootLogger,
    ...brokerPoolOptions
  }) {
    super({ ...brokerPoolOptions, logger: rootLogger })
    this.instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
    this.logger = rootLogger.namespace('ConnectionPool')
  }

  /** @type {import("../../types").ConnectionPool["on"]} */
  on(eventName, listener) {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return this.instrumentationEmitter.addListener(unwrapEvent(eventName), event => {
      event.type = wrapEvent(event.type)
      Promise.resolve(listener(event)).catch(e => {
        this.logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
        })
      })
    })
  }

  get events() {
    return events
  }
}
