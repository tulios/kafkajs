const { createConnection, connectionOpts, saslEntries, newLogger } = require('testHelpers')

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

  for (const e of saslEntries) {
    test(`when authenticated with SASL ${e.name} set authenticated to false`, async () => {
      broker = new Broker({
        connection: createConnection(e.opts()),
        logger: newLogger(),
      })
      await broker.connect()
      expect(broker.authenticatedAt).not.toBe(null)
      await broker.disconnect()
      expect(broker.authenticatedAt).toBe(null)
    })
  }
})
