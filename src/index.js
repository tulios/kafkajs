const { createLogger, LEVELS: { INFO } } = require('./loggers/console')
const createProducer = require('./producer')

module.exports = class Client {
  constructor({ host, port, ssl, logLevel = INFO }) {
    this.host = host
    this.port = port
    this.ssl = ssl
    this.logger = createLogger({ level: logLevel })
  }

  producer({ createPartitioner } = {}) {
    return createProducer({
      host: this.host,
      port: this.port,
      ssl: this.ssl,
      logger: this.logger,
      createPartitioner,
    })
  }
}
