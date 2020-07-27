const { requests, lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const PlainAuthenticator = require('./plain')
const SCRAM256Authenticator = require('./scram256')
const SCRAM512Authenticator = require('./scram512')
const AWSIAMAuthenticator = require('./awsIam')
const OAuthBearerAuthenticator = require('./oauthBearer')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

const AUTHENTICATORS = {
  PLAIN: PlainAuthenticator,
  'SCRAM-SHA-256': SCRAM256Authenticator,
  'SCRAM-SHA-512': SCRAM512Authenticator,
  AWS: AWSIAMAuthenticator,
  OAUTHBEARER: OAuthBearerAuthenticator,
}

const SUPPORTED_MECHANISMS = Object.keys(AUTHENTICATORS)
const UNLIMITED_SESSION_LIFETIME = '0'

module.exports = class SASLAuthenticator {
  constructor(connection, logger, versions, supportAuthenticationProtocol) {
    this.connection = connection
    this.logger = logger
    this.sessionLifetime = UNLIMITED_SESSION_LIFETIME

    const lookupRequest = lookup(versions)
    this.saslHandshake = lookupRequest(apiKeys.SaslHandshake, requests.SaslHandshake)
    this.protocolAuthentication = supportAuthenticationProtocol
      ? lookupRequest(apiKeys.SaslAuthenticate, requests.SaslAuthenticate)
      : null
  }

  async authenticate() {
    const mechanism = this.connection.sasl.mechanism.toUpperCase()
    if (!SUPPORTED_MECHANISMS.includes(mechanism)) {
      throw new KafkaJSSASLAuthenticationError(
        `SASL ${mechanism} mechanism is not supported by the client`
      )
    }

    const handshake = await this.connection.send(this.saslHandshake({ mechanism }))
    if (!handshake.enabledMechanisms.includes(mechanism)) {
      throw new KafkaJSSASLAuthenticationError(
        `SASL ${mechanism} mechanism is not supported by the server`
      )
    }

    const saslAuthenticate = async ({ request, response, authExpectResponse }) => {
      if (this.protocolAuthentication) {
        const { buffer: requestAuthBytes } = await request.encode()
        const authResponse = await this.connection.send(
          this.protocolAuthentication({ authBytes: requestAuthBytes })
        )

        // `0` is a string because `sessionLifetimeMs` is an int64 encoded as string.
        // This is not present in SaslAuthenticateV0, so we default to `"0"`
        this.sessionLifetime = authResponse.sessionLifetimeMs || UNLIMITED_SESSION_LIFETIME

        if (!authExpectResponse) {
          return
        }

        const { authBytes: responseAuthBytes } = authResponse
        const payloadDecoded = await response.decode(responseAuthBytes)
        return response.parse(payloadDecoded)
      }

      return this.connection.authenticate({ request, response, authExpectResponse })
    }

    const Authenticator = AUTHENTICATORS[mechanism]
    await new Authenticator(this.connection, this.logger, saslAuthenticate).authenticate()
  }
}
