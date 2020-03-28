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
    if (sasl.oauthBearerResolver == null) {
      throw new KafkaJSSASLAuthenticationError(
        'SASL OAUTHBEARER: Missing OAuth Bearer Token resolver'
      )
    }

    const { oauthBearerResolver } = sasl

    const oauthBearerToken = await oauthBearerResolver()

    if (
      oauthBearerToken.value == null ||
      oauthBearerToken.scope == null ||
      oauthBearerToken.lifetimeMs == null ||
      oauthBearerToken.principalName == null ||
      oauthBearerToken.startTimeMs == null
    ) {
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
