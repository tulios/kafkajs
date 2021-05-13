const { request } = require('../../protocol/sasl/awsIam')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

function AWSIAMAuthenticator({ sasl, connection, logger, saslAuthenticate }) {
  return {
    authenticate: async () => {
      if (!sasl.authorizationIdentity) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing authorizationIdentity')
      }
      if (!sasl.accessKeyId) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing accessKeyId')
      }
      if (!sasl.secretAccessKey) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing secretAccessKey')
      }
      if (!sasl.sessionToken) {
        sasl.sessionToken = ''
      }

      const { host, port } = connection
      const broker = `${host}:${port}`

      try {
        logger.debug('Authenticate with SASL AWS-IAM', { broker })
        await saslAuthenticate({ request: request(sasl) })
        logger.debug('SASL AWS-IAM authentication successful', { broker })
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(
          `SASL AWS-IAM authentication failed: ${e.message}`
        )
        logger.error(error.message, { broker })
        throw error
      }
    },
  }
}

module.exports = AWSIAMAuthenticator
