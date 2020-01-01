const {
  createConnection,
  connectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  newLogger,
  testIfKafka_1_1_0,
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

  test('authenticate with SASL PLAIN if configured', async () => {
    broker = new Broker({
      connection: createConnection(saslConnectionOpts()),
      logger: newLogger(),
    })
    expect(broker.isConnected()).toEqual(false)
    await broker.connect()
    expect(broker.isConnected()).toEqual(true)
  })

  test('authenticate with SASL SCRAM 256 if configured', async () => {
    broker = new Broker({
      connection: createConnection(saslSCRAM256ConnectionOpts()),
      logger: newLogger(),
    })
    expect(broker.isConnected()).toEqual(false)
    await broker.connect()
    expect(broker.isConnected()).toEqual(true)
  })

  test('authenticate with SASL SCRAM 512 if configured', async () => {
    broker = new Broker({
      connection: createConnection(saslSCRAM512ConnectionOpts()),
      logger: newLogger(),
    })
    expect(broker.isConnected()).toEqual(false)
    await broker.connect()
    expect(broker.isConnected()).toEqual(true)
  })

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

    test('returns false when connected but not authenticated on connections with SASL', async () => {
      broker = new Broker({
        connection: createConnection(saslConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connection.connect()
      expect(broker.isConnected()).toEqual(false)
    })

    test('returns true when connected', async () => {
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })

    test('returns true when connected and authenticated on connections with SASL', async () => {
      broker = new Broker({
        connection: createConnection(saslConnectionOpts()),
        logger: newLogger(),
      })
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })

    test('returns false when the session lifetime has expired', async () => {
      const sessionLifetime = 15000
      const reauthenticationThreshold = 10000
      broker = new Broker({
        connection: createConnection(saslConnectionOpts()),
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
  })

  test('returns true when the session lifetime is 0', async () => {
    broker = new Broker({
      connection: createConnection(saslConnectionOpts()),
      logger: newLogger(),
    })

    await broker.connect()
    expect(broker.isConnected()).toEqual(true)

    broker.sessionLifetime = Long.ZERO
    broker.authenticatedAt = [0, 0]

    expect(broker.isConnected()).toEqual(true)
  })

  describe('when SaslAuthenticate protocol is available', () => {
    testIfKafka_1_1_0('authenticate with SASL PLAIN if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('authenticate with SASL SCRAM 256 if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslSCRAM256ConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('authenticate with SASL SCRAM 512 if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslSCRAM512ConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('parallel calls to connect using SCRAM', async () => {
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
