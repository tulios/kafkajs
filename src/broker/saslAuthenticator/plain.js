const { request } = require('../../protocol/sasl/plain')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

function PlainAuthenticator({ sasl, connection, logger, saslAuthenticate }) {
  return {
    authenticate: async () => {
      if (sasl.username == null || sasl.password == null) {
        throw new KafkaJSSASLAuthenticationError('SASL Plain: Invalid username or password')
      }

      const { host, port } = connection
      const broker = `${host}:${port}`

      try {
        logger.debug('Authenticate with SASL PLAIN', { broker })
        await saslAuthenticate({ request: request(sasl) })
        logger.debug('SASL PLAIN authentication successful', { broker })
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(
          `SASL PLAIN authentication failed: ${e.message}`
        )
        logger.error(error.message, { broker })
        throw error
      }
    },
  }
}

module.exports = PlainAuthenticator
