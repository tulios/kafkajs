const { createLogger, LEVELS: { INFO } } = require('./loggers')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')

const { assign } = Object

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    retry,
    logLevel = INFO,
    logCreator = LoggerConsole,
  }) {
    this.logger = createLogger({ level: logLevel, logCreator })
    this.createCluster = () =>
      new Cluster({
        logger: this.logger,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        retry,
      })
  }

  /**
   * @public
   */
  producer({ createPartitioner, retry } = {}) {
    const cluster = this.createCluster()
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
      createPartitionAssigner,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      retry,
    } = {}
  ) {
    const cluster = this.createCluster()
    return createConsumer({
      retry: assign({}, cluster.retry, retry),
      logger: this.logger,
      cluster,
      groupId,
      createPartitionAssigner,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
    })
  }
}
