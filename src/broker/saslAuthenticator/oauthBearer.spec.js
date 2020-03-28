const { newLogger } = require('testHelpers')
const OAuthBearer = require('./oauthBearer')

describe('Broker > SASL Authenticator > OAUTHBEARER', () => {
  it('throws KafkaJSSASLAuthenticationError for missing oauthBearerProvider', async () => {
    const oauthBearer = new OAuthBearer({ sasl: {} }, newLogger())
    await expect(oauthBearer.authenticate()).rejects.toThrow('Missing OAuth bearer token provider')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid OAuth bearer token', async () => {
    async function oauthBearerProvider() {
      return {}
    }

    const oauthBearer = new OAuthBearer({ sasl: { oauthBearerProvider } }, newLogger())
    await expect(oauthBearer.authenticate()).rejects.toThrow('Invalid OAuth bearer token')
  })
})
