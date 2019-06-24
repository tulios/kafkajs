const awsIam = require('../../protocol/sasl/awsIam')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

module.exports = class AWSIAMAuthenticator {
  constructor(connection, logger, saslAuthenticate) {
    this.connection = connection
    this.logger = logger.namespace('SASLAWSIAMAuthenticator')
    this.saslAuthenticate = saslAuthenticate
  }

  async authenticate() {
    const { sasl } = this.connection
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

    const request = awsIam.request(sasl)
    const response = awsIam.response
    const { host, port } = this.connection
    const broker = `${host}:${port}`

    try {
      this.logger.debug('Authenticate with SASL AWS-IAM', { broker })
      await this.saslAuthenticate({ request, response })
      this.logger.debug('SASL AWS-IAM authentication successful', { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(
        `SASL AWS-IAM authentication failed: ${e.message}`
      )
      this.logger.error(error.message, { broker })
      throw error
    }
  }
}
