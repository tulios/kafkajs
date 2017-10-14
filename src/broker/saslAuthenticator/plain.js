const plain = require('../../protocol/sasl/plain')

module.exports = class PlainAuthenticator {
  constructor(connection) {
    this.connection = connection
  }

  async authenticate() {
    const request = plain.request(this.connection.sasl)
    const response = plain.response
    const { logger, host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      logger.debug('Authenticate with SASL PLAIN', { broker })
      await this.connection.authenticate({ request, response })
      logger.debug('SASL PLAIN authentication successful', { broker })
    } catch (e) {
      const error = new Error(`SASL PLAIN authentication failed: ${e.message}`)
      logger.error(error.message, { broker })
      throw error
    }
  }
}
