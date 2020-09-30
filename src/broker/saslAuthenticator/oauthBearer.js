/**
 * The sasl object must include a property named oauthBearerProvider, an
 * async function that is used to return the OAuth bearer token.
 *
 * The OAuth bearer token must be an object with properties value and
 * (optionally) extensions, that will be sent during the SASL/OAUTHBEARER
 * request.
 *
 * The implementation of the oauthBearerProvider must take care that tokens are
 * reused and refreshed when appropriate.
 */

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
        'SASL OAUTHBEARER: Missing OAuth bearer token provider'
      )
    }

    const { oauthBearerProvider } = sasl

    const oauthBearerToken = await oauthBearerProvider()

    if (oauthBearerToken.value == null) {
      throw new KafkaJSSASLAuthenticationError('SASL OAUTHBEARER: Invalid OAuth bearer token')
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
