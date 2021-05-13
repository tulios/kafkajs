const { newLogger } = require('testHelpers')
const Plain = require('./plain')

describe('Broker > SASL Authenticator > PLAIN', () => {
  it('throws KafkaJSSASLAuthenticationError for invalid username', async () => {
    const plain = Plain({
      sasl: {},
      connection: {},
      logger: newLogger(),
      saslAuthenticate: async () => {},
    })
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid password', async () => {
    const plain = Plain({
      sasl: { username: '<username>' },
      connection: {},
      logger: newLogger(),
      saslAuthenticate: async () => {},
    })
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })
})
