const { newLogger } = require('testHelpers')
const AWSIAM = require('./awsIam')

describe('Broker > SASL Authenticator > AWS-IAM', () => {
  it('throws KafkaJSSASLAuthenticationError for missing authorizationIdentity', async () => {
    const awsIam = new AWSIAM({ sasl: {} }, newLogger())
    await expect(awsIam.authenticate()).rejects.toThrow(
      'SASL AWS-IAM: Missing authorizationIdentity'
    )
  })

  it('throws KafkaJSSASLAuthenticationError for invalid accessKeyId', async () => {
    const awsIam = new AWSIAM(
      {
        sasl: {
          authorizationIdentity: '<authorizationIdentity>',
          secretAccessKey: '<secretAccessKey>',
        },
      },
      newLogger()
    )
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing accessKeyId')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid secretAccessKey', async () => {
    const awsIam = new AWSIAM(
      { sasl: { authorizationIdentity: '<authorizationIdentity>', accessKeyId: '<accessKeyId>' } },
      newLogger()
    )
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing secretAccessKey')
  })
})
