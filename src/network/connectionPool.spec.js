const apiKeys = require('../protocol/requests/apiKeys')
const { connectionOpts } = require('testHelpers')
const ConnectionPool = require('./connectionPool')

describe('ConnectionPool', () => {
  /** @type {ConnectionPool} */
  let pool

  beforeEach(() => {
    pool = new ConnectionPool(connectionOpts())
  })

  afterEach(async () => {
    pool && (await pool.destroy())
  })

  it('should connect on getConnect()', async () => {
    const connection = await pool.getConnection()
    expect(connection.isConnected()).toBe(true)

    expect(pool.isConnected()).toBe(true)
    await pool.destroy()
    expect(pool.isConnected()).toBe(false)

    expect(connection.isConnected()).toBe(false)
  })

  it('should return different connection for Fetch requests', async () => {
    const heartbeatConnection = pool.getConnectionByRequest({
      request: { apiKey: apiKeys.Heartbeat },
    })
    const fetchConnection = pool.getConnectionByRequest({ request: { apiKey: apiKeys.Fetch } })
    expect(heartbeatConnection !== fetchConnection).toBe(true)
  })

  it('should reconnect connection if it disconnects', async () => {
    let connection = await pool.getConnection()

    expect(connection.isConnected()).toBe(true)
    await connection.disconnect()
    expect(connection.isConnected()).toBe(false)

    connection = await pool.getConnection()
    expect(connection.isConnected()).toBe(true)
  })
})
