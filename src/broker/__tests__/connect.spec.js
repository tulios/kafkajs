const {
  createConnection,
  connectionOpts,
  saslSCRAM256ConnectionOpts,
  newLogger,
  testIfKafkaAtLeast_1_1_0,
  describeIfOauthbearerDisabled,
  saslEntries,
} = require('testHelpers')

const Long = require('../../utils/long')
const Broker = require('../index')

describe('Broker > connect', () => {
  let broker

  beforeEach(() => {
    broker = new Broker({
      connection: createConnection(connectionOpts()),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('establish the connection', async () => {
    await broker.connect()
    expect(broker.connection.connected).toEqual(true)
  })

  test('load api versions if not provided', async () => {
    expect(broker.versions).toEqual(null)
    await broker.connect()
    expect(broker.versions).toBeTruthy()
  })

  for (const e of saslEntries) {
    test(`authenticate with SASL ${e.name} if configured`, async () => {
      broker = new Broker({
        connection: createConnection(e.opts()),
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
        connection: createConnection(saslSCRAM256ConnectionOpts()),
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

  test('sets the authenticatedAt timer', async () => {
    const error = new Error('not connected')
    const timer = process.hrtime()
    broker.authenticatedAt = timer
    broker.connection.connect = jest.fn(() => {
      throw error
    })

    expect(broker.authenticatedAt).toEqual(timer)
    await expect(broker.connect()).rejects.toEqual(error)
    expect(broker.authenticatedAt).toBe(null)
  })

  describe('#isConnected', () => {
    test('returns false when not connected', () => {
      expect(broker.isConnected()).toEqual(false)
    })

    for (const e of saslEntries) {
      test(`returns false when connected but not authenticated on connections with SASL ${e.name}`, async () => {
        broker = new Broker({
          connection: createConnection(e.opts()),
          logger: newLogger(),
        })
        expect(broker.isConnected()).toEqual(false)
        await broker.connection.connect()
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
          broker = new Broker({
            connection: createConnection(e.opts()),
            logger: newLogger(),
          })
          await broker.connect()
          expect(broker.isConnected()).toEqual(true)
        })

        test('returns false when the session lifetime has expired', async () => {
          const sessionLifetime = 15000
          const reauthenticationThreshold = 10000
          broker = new Broker({
            connection: createConnection(e.opts()),
            logger: newLogger(),
            reauthenticationThreshold,
          })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          broker.sessionLifetime = Long.fromValue(sessionLifetime)
          const [seconds] = broker.authenticatedAt
          broker.authenticatedAt = [seconds - sessionLifetime / 1000, 0]

          expect(broker.isConnected()).toEqual(false)
        })

        test('returns true when the session lifetime is 0', async () => {
          broker = new Broker({
            connection: createConnection(e.opts()),
            logger: newLogger(),
          })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          broker.sessionLifetime = Long.ZERO
          broker.authenticatedAt = [0, 0]

          expect(broker.isConnected()).toEqual(true)
        })

        testIfKafkaAtLeast_1_1_0(`authenticate with SASL ${e.name} if configured`, async () => {
          broker = new Broker({
            connection: createConnection(e.opts()),
            logger: newLogger(),
          })
          expect(broker.isConnected()).toEqual(false)
          await broker.connect()
          expect(broker.isConnected()).toEqual(true)
          expect(broker.supportAuthenticationProtocol).toEqual(true)
        })
      }
    })

    describeIfOauthbearerDisabled('when SASL SCRAM is configured', () => {
      testIfKafkaAtLeast_1_1_0('parallel calls to connect using SCRAM', async () => {
        broker = new Broker({
          connection: createConnection(saslSCRAM256ConnectionOpts()),
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
