const { newLogger } = require('testHelpers')
const awsIAMAuthenticatorProvider = require('./awsIam')

describe('Broker > SASL Authenticator > AWS-IAM', () => {
  it('throws KafkaJSSASLAuthenticationError for missing authorizationIdentity', async () => {
    const awsIam = awsIAMAuthenticatorProvider({})({ host: '', port: 0, logger: newLogger() })
    await expect(awsIam.authenticate()).rejects.toThrow(
      'SASL AWS-IAM: Missing authorizationIdentity'
    )
  })

  it('throws KafkaJSSASLAuthenticationError for invalid accessKeyId', async () => {
    const awsIam = awsIAMAuthenticatorProvider({
      authorizationIdentity: '<authorizationIdentity>',
      secretAccessKey: '<secretAccessKey>',
    })({ host: '', port: 0, logger: newLogger() })
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing accessKeyId')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid secretAccessKey', async () => {
    const awsIam = awsIAMAuthenticatorProvider({
      authorizationIdentity: '<authorizationIdentity>',
      accessKeyId: '<accessKeyId>',
    })({ host: '', port: 0, logger: newLogger() })
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing secretAccessKey')
  })
})
