const {
  createConnection,
  connectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  newLogger,
  testIfKafka_1_1_0,
} = require('testHelpers')

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
    expect(broker.authenticated).toEqual(false)
    await broker.connect()
    expect(broker.authenticated).toEqual(true)
  })

  test('authenticate with SASL SCRAM 256 if configured', async () => {
    broker = new Broker({
      connection: createConnection(saslSCRAM256ConnectionOpts()),
      logger: newLogger(),
    })
    expect(broker.authenticated).toEqual(false)
    await broker.connect()
    expect(broker.authenticated).toEqual(true)
  })

  test('authenticate with SASL SCRAM 512 if configured', async () => {
    broker = new Broker({
      connection: createConnection(saslSCRAM512ConnectionOpts()),
      logger: newLogger(),
    })
    expect(broker.authenticated).toEqual(false)
    await broker.connect()
    expect(broker.authenticated).toEqual(true)
  })

  test('parallel calls to connect using SCRAM', async () => {
    broker = new Broker({
      connection: createConnection(saslSCRAM256ConnectionOpts()),
      logger: newLogger(),
    })

    expect(broker.authenticated).toEqual(false)

    await Promise.all([
      broker.connect(),
      broker.connect(),
      broker.connect(),
      broker.connect(),
      broker.connect(),
    ])

    expect(broker.authenticated).toEqual(true)
  })

  test('switches the authenticated flag to false', async () => {
    const error = new Error('not connected')
    broker.authenticated = true
    broker.connection.connect = jest.fn(() => {
      throw error
    })

    expect(broker.authenticated).toEqual(true)
    await expect(broker.connect()).rejects.toEqual(error)
    expect(broker.authenticated).toEqual(false)
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
  })

  describe('when SaslAuthenticate protocol is available', () => {
    testIfKafka_1_1_0('authenticate with SASL PLAIN if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.authenticated).toEqual(false)
      await broker.connect()
      expect(broker.authenticated).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('authenticate with SASL SCRAM 256 if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslSCRAM256ConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.authenticated).toEqual(false)
      await broker.connect()
      expect(broker.authenticated).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('authenticate with SASL SCRAM 512 if configured', async () => {
      broker = new Broker({
        connection: createConnection(saslSCRAM512ConnectionOpts()),
        logger: newLogger(),
      })
      expect(broker.authenticated).toEqual(false)
      await broker.connect()
      expect(broker.authenticated).toEqual(true)
      expect(broker.supportAuthenticationProtocol).toEqual(true)
    })

    testIfKafka_1_1_0('parallel calls to connect using SCRAM', async () => {
      broker = new Broker({
        connection: createConnection(saslSCRAM256ConnectionOpts()),
        logger: newLogger(),
      })

      expect(broker.authenticated).toEqual(false)

      await Promise.all([
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
      ])

      expect(broker.authenticated).toEqual(true)
    })
  })
})
