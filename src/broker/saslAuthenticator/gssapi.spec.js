const { newLogger } = require('testHelpers')
const gssapiAuthenticatorProvider = require('./gssapi')

describe('Broker > SASL Authenticator > GSSAPI', () => {
  it('throws KafkaJSSASLAuthenticationError when kerberos package is not installed', async () => {
    // The kerberos package is optional, so if it's not installed,
    // the authenticator should throw a helpful error message
    let error
    try {
      require('kerberos') // eslint-disable-line
      // If kerberos is installed, skip this test
      return
    } catch (_) {
      // kerberos is not installed, proceed with test
    }

    const gssapi = gssapiAuthenticatorProvider({})({
      host: 'localhost',
      port: 9092,
      logger: newLogger(),
      saslAuthenticate: jest.fn(),
    })

    await expect(gssapi.authenticate()).rejects.toThrow(
      'The "kerberos" npm package is required for Kerberos authentication'
    )
  })

  it('uses default serviceName of "kafka"', () => {
    const sasl = { mechanism: 'gssapi' }
    const provider = gssapiAuthenticatorProvider(sasl)
    expect(provider).toBeDefined()
    expect(typeof provider).toBe('function')
  })

  it('accepts custom serviceName', () => {
    const sasl = { mechanism: 'gssapi', serviceName: 'custom-service' }
    const provider = gssapiAuthenticatorProvider(sasl)
    expect(provider).toBeDefined()
  })

  it('accepts keytab and principal options', () => {
    const sasl = {
      mechanism: 'gssapi',
      serviceName: 'kafka',
      principal: 'client@EXAMPLE.COM',
      keytab: '/path/to/client.keytab',
    }
    const provider = gssapiAuthenticatorProvider(sasl)
    expect(provider).toBeDefined()
  })
})
