const { createLogger, LEVELS: { INFO } } = require('./loggers')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')

const { assign } = Object
const privateCreateCluster = Symbol('private:Kafka:createCluster')

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    retry,
    logLevel = INFO,
    logCreator = LoggerConsole,
    allowExperimentalV011 = false,
  }) {
    this.logger = createLogger({ level: logLevel, logCreator })
    this[privateCreateCluster] = (metadataMaxAge = 300000) =>
      new Cluster({
        logger: this.logger,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        metadataMaxAge,
        retry,
        allowExperimentalV011,
      })
  }

  /**
   * @public
   */
  producer({ createPartitioner, retry, metadataMaxAge } = {}) {
    const cluster = this[privateCreateCluster](metadataMaxAge)
    return createProducer({
      retry: assign({}, cluster.retry, retry),
      logger: this.logger,
      cluster,
      createPartitioner,
    })
  }

  /**
   * @public
   */
  consumer(
    {
      groupId,
      partitionAssigners,
      metadataMaxAge,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      retry,
    } = {}
  ) {
    const cluster = this[privateCreateCluster](metadataMaxAge)
    return createConsumer({
      retry: assign({}, cluster.retry, retry),
      logger: this.logger,
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const cluster = this[privateCreateCluster]()
    return createAdmin({
      retry: assign({}, cluster.retry, retry),
      logger: this.logger,
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this.logger
  }
}
