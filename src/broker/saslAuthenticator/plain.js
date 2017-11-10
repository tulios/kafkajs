const plain = require('../../protocol/sasl/plain')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class PlainAuthenticator {
  constructor(connection, logger) {
    this.connection = connection
    this.logger = logger.namespace('SASLPlainAuthenticator')
  }

  async authenticate() {
    const request = plain.request(this.connection.sasl)
    const response = plain.response
    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL PLAIN', { broker })
      await this.connection.authenticate({ request, response })
      this.logger.debug('SASL PLAIN authentication successful', { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL PLAIN authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
