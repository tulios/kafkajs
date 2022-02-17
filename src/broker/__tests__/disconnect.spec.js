const { createConnectionPool, connectionOpts, saslEntries, newLogger } = require('testHelpers')

const Broker = require('../index')

describe('Broker > disconnect', () => {
  let broker

  beforeEach(() => {
    broker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('disconnect', async () => {
    await broker.connect()
    expect(broker.connectionPool.isConnected()).toEqual(true)
    await broker.disconnect()
    expect(broker.connectionPool.isConnected()).toEqual(false)
  })

  for (const e of saslEntries) {
    test(`when authenticated with SASL ${e.name} set authenticated to false`, async () => {
      const connectionPool = createConnectionPool(e.opts())
      broker = new Broker({ connectionPool, logger: newLogger() })
      await broker.connect()

      const connection = await connectionPool.getConnection()
      expect(connection.authenticatedAt).not.toBe(null)

      await broker.disconnect()
      expect(connection.authenticatedAt).toBe(null)
    })
  }
})
