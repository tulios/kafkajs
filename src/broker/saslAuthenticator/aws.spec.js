const { newLogger } = require('testHelpers')
const AWS = require('./aws')

describe('Broker > SASL Authenticator > AWS', () => {
  it('throws KafkaJSSASLAuthenticationError for missing authorizationIdentity', async () => {
    const aws = new AWS({ sasl: {} }, newLogger())
    await expect(aws.authenticate()).rejects.toThrow('SASL AWS: Missing authorizationIdentity')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid accessKeyId', async () => {
    const aws = new AWS(
      {
        sasl: {
          authorizationIdentity: '<authorizationIdentity>',
          secretAccessKey: '<secretAccessKey>',
        },
      },
      newLogger()
    )
    await expect(aws.authenticate()).rejects.toThrow('SASL AWS: Missing accessKeyId')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid secretAccessKey', async () => {
    const aws = new AWS(
      { sasl: { authorizationIdentity: '<authorizationIdentity>', accessKeyId: '<accessKeyId>' } },
      newLogger()
    )
    await expect(aws.authenticate()).rejects.toThrow('SASL AWS: Missing secretAccessKey')
  })
})
