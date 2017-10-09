const plain = require('../../protocol/sasl/plain')

module.exports = class PlainAuthenticator {
  constructor(connection) {
    this.connection = connection
  }

  async authenticate() {
    const request = plain.request(this.connection.sasl)
    const response = plain.response
    const { logger } = this.connection

    try {
      logger.debug('Authenticate with SASL PLAIN', { broker: this.connection.broker })
      await this.connection.authenticate({ request, response })
      logger.debug('SASL PLAIN authentication successful', { broker: this.connection.broker })
    } catch (e) {
      throw new Error(`SASL PLAIN authentication failed: ${e.message}`)
    }
  }
}
