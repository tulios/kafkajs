const { createLogger, LEVELS: { INFO } } = require('./loggers/console')
const createProducer = require('./producer')

module.exports = class Client {
  constructor({ host, port, ssl, sasl, logLevel = INFO }) {
    this.host = host
    this.port = port
    this.ssl = ssl
    this.sasl = sasl
    this.logger = createLogger({ level: logLevel })
  }

  producer({ createPartitioner } = {}) {
    return createProducer({
      host: this.host,
      port: this.port,
      ssl: this.ssl,
      sasl: this.sasl,
      logger: this.logger,
      createPartitioner,
    })
  }
}
