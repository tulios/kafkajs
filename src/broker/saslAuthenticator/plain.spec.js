const { newLogger } = require('testHelpers')
const Plain = require('./plain')

describe('Broker > SASL Authenticator > PLAIN', () => {
  it('throws KafkaJSSASLAuthenticationError for invalid username', async () => {
    const plain = new Plain({ sasl: {} }, newLogger())
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid password', async () => {
    const plain = new Plain({ sasl: { username: '<username>' } }, newLogger())
    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password')
  })
})
