const { createLogger, LEVELS: { INFO } } = require('./loggers')
const logFunctionConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    retry,
    logLevel = INFO,
    logFunction = logFunctionConsole,
  }) {
    this.logger = createLogger({ level: logLevel, logFunction })
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
    return createProducer({
      cluster: this.createCluster(),
      logger: this.logger,
      createPartitioner,
      retry,
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
    return createConsumer({
      cluster: this.createCluster(),
      logger: this.logger,
      groupId,
      createPartitionAssigner,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      retry,
    })
  }
}
