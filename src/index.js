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
    this.cluster = new Cluster({
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
      logger: this.logger,
    })
  }

  producer({ createPartitioner, retry } = {}) {
    return createProducer({
      cluster: this.cluster,
      logger: this.logger,
      createPartitioner,
      retry,
    })
  }

  consumer({ groupId, createPartitionAssigner, sessionTimeout, retry } = {}) {
    return createConsumer({
      cluster: this.cluster,
      logger: this.logger,
      groupId,
      createPartitionAssigner,
      sessionTimeout,
      retry,
    })
  }
}
