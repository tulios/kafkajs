const oauthBearer = require('../../protocol/sasl/oauthBearer')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class OAuthBearerAuthenticator {
  constructor(connection, logger, saslAuthenticate) {
    this.connection = connection
    this.logger = logger.namespace('SASLOAuthBearerAuthenticator')
    this.saslAuthenticate = saslAuthenticate
  }

  async authenticate() {
    const { sasl } = this.connection
    if (sasl.oauthBearerProvider == null) {
      throw new KafkaJSSASLAuthenticationError(
        'SASL OAUTHBEARER: Missing OAuth Bearer Token provider'
      )
    }

    const { oauthBearerProvider } = sasl

    const oauthBearerToken = await oauthBearerProvider()

    if (oauthBearerToken.value == null) {
      throw new KafkaJSSASLAuthenticationError('SASL OAUTHBEARER: Invalid OAuth Bearer Token')
    }

    const request = await oauthBearer.request(sasl, oauthBearerToken)
    const response = oauthBearer.response
    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL OAUTHBEARER', { broker })
      await this.saslAuthenticate({ request, response })
      this.logger.debug('SASL OAUTHBEARER authentication successful', { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL OAUTHBEARER authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
