const Decoder = require('../../protocol/decoder')
const { newLogger } = require('testHelpers')
const SCRAM256 = require('./scram256')

describe('Broker > SASL Authenticator > SCRAM', () => {
  let connection, saslAuthenticate, logger

  beforeEach(() => {
    connection = {
      authenticate: jest.fn(),
      sasl: { username: 'user', password: 'pencil' },
    }
    saslAuthenticate = ({ request, response, authExpectResponse }) =>
      connection.authenticate({ request, response, authExpectResponse })

    logger = { debug: jest.fn() }
    logger.namespace = () => logger
  })

  it('throws KafkaJSSASLAuthenticationError for invalid username', async () => {
    const scram = new SCRAM256({ sasl: {} }, newLogger(), saslAuthenticate)
    await expect(scram.authenticate()).rejects.toThrow('Invalid username or password')
  })

  it('throws KafkaJSSASLAuthenticationError for invalid password', async () => {
    const scram = new SCRAM256({ sasl: { username: '<username>' } }, newLogger(), saslAuthenticate)
    await expect(scram.authenticate()).rejects.toThrow('Invalid username or password')
  })

  describe('SCRAM 256', () => {
    let scram

    beforeEach(() => {
      scram = new SCRAM256(connection, logger, saslAuthenticate)
    })

    test('saltPassword', async () => {
      connection.sasl.password = 'password'
      const clientMessageResponse = {
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      }
      const saltedPassword = await scram.saltPassword(clientMessageResponse)
      expect(saltedPassword.toString('hex')).toEqual(
        '72c2aaf3a8fd5732b83c5bd9fbf8d0c6e851d8d18d56fbb4e73813acf267009e'
      )
    })

    test('clientKey', async () => {
      connection.sasl.password = 'password'
      const clientMessageResponse = {
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      }
      const clientKey = await scram.clientKey(clientMessageResponse)
      expect(clientKey.toString('hex')).toEqual(
        '21819e176123554b9cec1dc1799b25ba112ae3c1d80e2b693476d28d99a15193'
      )
    })

    test('storedKey', async () => {
      connection.sasl.password = 'password'
      const clientMessageResponse = {
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      }
      const clientKey = await scram.clientKey(clientMessageResponse)
      const storedKey = scram.H(clientKey)
      expect(storedKey.toString('hex')).toEqual(
        '228713ebcc6a14f44503e9a0ecfe01d9e6b88adb39b890ade8b222fa4c323fd9'
      )
    })

    describe('first message', () => {
      test('regular use case', async () => {
        scram.currentNonce = 'rOprNGfwEbeRWgbNEkqO'
        await scram.sendClientFirstMessage()
        expect(connection.authenticate).toHaveBeenCalledWith({
          authExpectResponse: true,
          request: expect.any(Object),
          response: expect.any(Object),
        })

        const { request } = connection.authenticate.mock.calls[0][0]
        const encoder = await request.encode()
        const decoder = new Decoder(encoder.buffer)
        expect(decoder.readBytes().toString()).toEqual(`n,,n=user,r=${scram.currentNonce}`)
      })

      test('username with comma', async () => {
        connection.sasl.username = 'bob,'
        await scram.sendClientFirstMessage()
        expect(connection.authenticate).toHaveBeenCalledWith({
          authExpectResponse: true,
          request: expect.any(Object),
          response: expect.any(Object),
        })

        const { request } = connection.authenticate.mock.calls[0][0]
        const encoder = await request.encode()
        const decoder = new Decoder(encoder.buffer)
        expect(decoder.readBytes().toString()).toEqual(`n,,n=bob=2C,r=${scram.currentNonce}`)
      })

      test('username with equals', async () => {
        connection.sasl.username = 'bob='
        await scram.sendClientFirstMessage()
        expect(connection.authenticate).toHaveBeenCalledWith({
          authExpectResponse: true,
          request: expect.any(Object),
          response: expect.any(Object),
        })

        const { request } = connection.authenticate.mock.calls[0][0]
        const encoder = await request.encode()
        const decoder = new Decoder(encoder.buffer)
        expect(decoder.readBytes().toString()).toEqual(`n,,n=bob=3D,r=${scram.currentNonce}`)
      })
    })

    describe('second message', () => {
      test('RFC5802#section-5 example data', async () => {
        scram.currentNonce = 'rOprNGfwEbeRWgbNEkqO'
        const clientMessageResponse = {
          original:
            'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
          r: 'rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0',
          s: 'W22ZaJ0SNY7soEsUEjb6gQ',
          i: '4096',
        }

        await scram.sendClientFinalMessage(clientMessageResponse)
        expect(connection.authenticate).toHaveBeenCalledWith({
          authExpectResponse: true,
          request: expect.any(Object),
          response: expect.any(Object),
        })

        const { request } = connection.authenticate.mock.calls[0][0]
        const encoder = await request.encode()
        const decoder = new Decoder(encoder.buffer)
        expect(decoder.readBytes().toString()).toEqual(
          'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='
        )
      })
    })
  })
})
