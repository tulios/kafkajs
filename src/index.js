const { createLogger, LEVELS: { INFO } } = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')

module.exports = class Client {
  constructor({ host, port, ssl, sasl, clientId, connectionTimeout, retry, logLevel = INFO }) {
    this.logger = createLogger({ level: logLevel })
    this.cluster = new Cluster({
      host,
      port,
      ssl,
      sasl,
      retry,
      logger: this.logger,
    })
  }

  producer({ createPartitioner, retry } = {}) {
    return createProducer({
      cluster: this.cluster,
      createPartitioner,
    })
  }
}
