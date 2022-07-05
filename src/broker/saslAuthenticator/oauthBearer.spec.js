const { newLogger } = require('testHelpers')
const oauthBearerAuthenticatorProvider = require('./oauthBearer')

describe('Broker > SASL Authenticator > OAUTHBEARER', () => {
  it('throws KafkaJSSASLAuthenticationError for missing oauthBearerProvider', async () => {
    const oauthBearer = oauthBearerAuthenticatorProvider({})({
      host: '',
      port: 0,
      logger: newLogger(),
    })
    await expect(oauthBearer.authenticate()).rejects.toThrow('Missing OAuth bearer token provider')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid OAuth bearer token', async () => {
    async function oauthBearerProvider() {
      return {}
    }

    const oauthBearer = oauthBearerAuthenticatorProvider({ oauthBearerProvider })({
      host: '',
      port: 0,
      logger: newLogger(),
    })
    await expect(oauthBearer.authenticate()).rejects.toThrow('Invalid OAuth bearer token')
  })
})
