const SCRAM = require('./scram')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class SCRAM256Authenticator extends SCRAM {
  constructor(connection, logger) {
    super(connection)
    this.logger = logger.namespace('SCRAM256Authenticator')
  }

  async authenticate() {
    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Exchanging first client message', { broker })
      await this.sendFirstClientMessage()
      // const firstMessageResponse = await this.sendFirstClientMessage()
      // TODO: send final message (with password and salt)
      // TODO: wrap up auth
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL SCRAM 256 authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
