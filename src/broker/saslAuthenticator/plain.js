const plain = require('../../protocol/sasl/plain')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class PlainAuthenticator {
  constructor(connection, logger, saslAuthenticate) {
    this.connection = connection
    this.logger = logger.namespace('SASLPlainAuthenticator')
    this.saslAuthenticate = saslAuthenticate
  }

  async authenticate() {
    const { sasl } = this.connection
    if (sasl.username == null || sasl.password == null) {
      throw new KafkaJSSASLAuthenticationError('SASL Plain: Invalid username or password')
    }

    const request = plain.request(sasl)
    const response = plain.response
    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL PLAIN', { broker })
      await this.saslAuthenticate({ request, response })
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
