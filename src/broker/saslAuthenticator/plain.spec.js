const { newLogger } = require('testHelpers')
const plainAuthenticatorProvider = require('./plain')

describe('Broker > SASL Authenticator > PLAIN', () => {
  it('throws KafkaJSSASLAuthenticationError for invalid username', async () => {
    const plain = plainAuthenticatorProvider({})('', 0, newLogger())
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid password', async () => {
    const plain = plainAuthenticatorProvider({ username: '<username>' })('', 0, newLogger())
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })
})
