const { createConnection, connectionOpts, saslConnectionOpts, newLogger } = require('testHelpers')
const Broker = require('../index')

describe('Broker > disconnect', () => {
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

  test('disconnect', async () => {
    await broker.connect()
    expect(broker.connection.connected).toEqual(true)
    await broker.disconnect()
    expect(broker.connection.connected).toEqual(false)
  })

  test('when authenticated with SASL set authenticated to false', async () => {
    broker = new Broker({
      connection: createConnection(saslConnectionOpts()),
      logger: newLogger(),
    })
    await broker.connect()
    expect(broker.authenticated).toEqual(true)
    await broker.disconnect()
    expect(broker.authenticated).toEqual(false)
  })
})
