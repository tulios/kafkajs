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

const { request } = require('../../protocol/sasl/oauthBearer')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

function OAuthBearerAuthenticator({ sasl, connection, logger, saslAuthenticate }) {
  return {
    authenticate: async () => {
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

      const { host, port } = connection
      const broker = `${host}:${port}`

      try {
        logger.debug('Authenticate with SASL OAUTHBEARER', { broker })
        await saslAuthenticate({ request: await request(sasl, oauthBearerToken) })
        logger.debug('SASL OAUTHBEARER authentication successful', { broker })
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(
          `SASL OAUTHBEARER authentication failed: ${e.message}`
        )
        logger.error(error.message, { broker })
        throw error
      }
    },
  }
}

module.exports = OAuthBearerAuthenticator
