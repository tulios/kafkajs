const { newLogger } = require('testHelpers')
const AWSIAM = require('./awsIam')

describe('Broker > SASL Authenticator > AWS-IAM', () => {
  it('throws KafkaJSSASLAuthenticationError for missing authorizationIdentity', async () => {
    const awsIam = AWSIAM({
      sasl: {},
      connection: { host: 'host', port: 9094 },
      logger: newLogger(),
      saslAuthenticate: () => {},
    })
    await expect(awsIam.authenticate()).rejects.toThrow(
      'SASL AWS-IAM: Missing authorizationIdentity'
    )
  })

  it('throws KafkaJSSASLAuthenticationError for invalid accessKeyId', async () => {
    const awsIam = AWSIAM({
      sasl: {
        authorizationIdentity: '<authorizationIdentity>',
        secretAccessKey: '<secretAccessKey>',
      },
      connection: {
        host: 'host',
        port: 9092,
      },
      logger: newLogger(),
    })
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing accessKeyId')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid secretAccessKey', async () => {
    const awsIam = AWSIAM({
      sasl: {
        authorizationIdentity: '<authorizationIdentity>',
        accessKeyId: '<accessKeyId>',
      },
      connection: {
        host: 'host',
        port: 9092,
      },
      logger: newLogger(),
    })
    await expect(awsIam.authenticate()).rejects.toThrow('SASL AWS-IAM: Missing secretAccessKey')
  })
})
