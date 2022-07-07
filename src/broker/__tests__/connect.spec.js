const {
  createConnectionPool,
  connectionOpts,
  saslSCRAM256ConnectionOpts,
  sslConnectionOpts,
  newLogger,
  testIfKafkaAtLeast_1_1_0,
  describeIfOauthbearerDisabled,
  saslEntries,
} = require('testHelpers')

const Long = require('../../utils/long')
const Broker = require('../index')

describe('Broker > connect', () => {
  let broker, connectionPool

  beforeEach(() => {
    connectionPool = createConnectionPool(connectionOpts())
    broker = new Broker({ connectionPool, logger: newLogger() })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('establish the connection', async () => {
    await broker.connect()
    expect(broker.connectionPool.isConnected()).toEqual(true)
  })

  test('load api versions if not provided', async () => {
    expect(broker.versions).toEqual(null)
    await broker.connect()
    expect(broker.versions).toBeTruthy()
  })

  test("throws if the mechanism isn't supported by the server", async () => {
    broker = new Broker({
      connectionPool: createConnectionPool(
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'fake-mechanism',
            authenticationProvider: () => ({
              authenticate: async () => {
                throw new Error('🥸')
              },
            }),
          },
        })
      ),
      logger: newLogger(),
    })

    await expect(broker.connect()).rejects.toThrow(
      'The broker does not support the requested SASL mechanism'
    )
  })

  describeIfOauthbearerDisabled('when PLAIN is configured', () => {
    test('user provided authenticator overrides built in ones', async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(
          Object.assign(sslConnectionOpts(), {
            port: 9094,
            sasl: {
              mechanism: 'PLAIN',
              authenticationProvider: () => ({
                authenticate: async () => {
                  throw new Error('test error')
                },
              }),
            },
          })
        ),
        logger: newLogger(),
      })

      await expect(broker.connect()).rejects.toThrow('test error')
    })
  })

  for (const e of saslEntries) {
    test(`authenticate with SASL ${e.name} if configured`, async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(e.opts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })
  }

  describeIfOauthbearerDisabled('when SASL SCRAM is configured', () => {
    test('parallel calls to connect using SCRAM', async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(saslSCRAM256ConnectionOpts()),
        logger: newLogger(),
      })

      expect(broker.isConnected()).toEqual(false)

      await Promise.all([
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
      ])

      expect(broker.isConnected()).toEqual(true)
    })
  })

  describe('#isConnected', () => {
    test('returns false when not connected', () => {
      expect(broker.isConnected()).toEqual(false)
    })

    for (const e of saslEntries) {
      test(`returns false when connected but not authenticated on connections with SASL ${e.name}`, async () => {
        const connectionPool = createConnectionPool(e.opts())
        broker = new Broker({ connectionPool, logger: newLogger() })
        expect(broker.isConnected()).toEqual(false)
        await connectionPool.getConnection()
        expect(broker.isConnected()).toEqual(false)
      })
    }

    test('returns true when connected', async () => {
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })

    describe('when SaslAuthenticate protocol is available', () => {
      for (const e of saslEntries) {
        test(`returns true when connected and authenticated on connections with SASL ${e.name}`, async () => {
          const connectionPool = createConnectionPool(e.opts())
          broker = new Broker({ connectionPool, logger: newLogger() })
          await broker.connect()
          expect(broker.isConnected()).toEqual(true)
        })

        test('returns false when the session lifetime has expired', async () => {
          const sessionLifetime = 15000
          const reauthenticationThreshold = 10000
          const connectionPool = createConnectionPool({ ...e.opts(), reauthenticationThreshold })
          broker = new Broker({ connectionPool, logger: newLogger() })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          const connection = await connectionPool.getConnection()

          connection.sessionLifetime = Long.fromValue(sessionLifetime)
          const [seconds] = connection.authenticatedAt
          connection.authenticatedAt = [seconds - sessionLifetime / 1000, 0]

          expect(broker.isConnected()).toEqual(false)
        })

        test('returns true when the session lifetime is 0', async () => {
          const connectionPool = createConnectionPool(e.opts())
          broker = new Broker({ connectionPool, logger: newLogger() })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          const connection = await connectionPool.getConnection()
          connection.sessionLifetime = Long.ZERO
          connection.authenticatedAt = [0, 0]

          expect(broker.isConnected()).toEqual(true)
        })

        testIfKafkaAtLeast_1_1_0(`authenticate with SASL ${e.name} if configured`, async () => {
          const connectionPool = createConnectionPool(e.opts())
          broker = new Broker({ connectionPool, logger: newLogger() })
          expect(broker.isConnected()).toEqual(false)
          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          const connection = await connectionPool.getConnection()
          expect(connection.getSupportAuthenticationProtocol()).toEqual(true)
        })
      }
    })

    describeIfOauthbearerDisabled('when SASL SCRAM is configured', () => {
      testIfKafkaAtLeast_1_1_0('parallel calls to connect using SCRAM', async () => {
        broker = new Broker({
          connectionPool: createConnectionPool(saslSCRAM256ConnectionOpts()),
          logger: newLogger(),
        })

        expect(broker.isConnected()).toEqual(false)

        await Promise.all([
          broker.connect(),
          broker.connect(),
          broker.connect(),
          broker.connect(),
          broker.connect(),
        ])

        expect(broker.isConnected()).toEqual(true)
      })
    })
  })
})
