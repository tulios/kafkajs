const { createLogger, LEVELS: { INFO } } = require('./loggers/console')
const createProducer = require('./producer')

module.exports = class Client {
  constructor({ host, port, logLevel = INFO }) {
    this.host = host
    this.port = port
    this.logger = createLogger({ level: logLevel })
  }

  producer() {
    return createProducer({
      host: this.host,
      port: this.port,
      logger: this.logger,
    })
  }
}
