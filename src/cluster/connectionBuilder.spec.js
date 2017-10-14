const connectionBuilder = require('./connectionBuilder')
const Connection = require('../network/connection')

describe('Cluster > ConnectionBuilder', () => {
  let builder

  const host = 'host.test'
  const port = 7777
  const ssl = { ssl: true }
  const sasl = { sasl: true }
  const clientId = 'test-client-id'
  const connectionTimeout = 30000
  const retry = { retry: true }
  const logger = jest.fn()

  beforeEach(() => {
    builder = connectionBuilder({
      host,
      port,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
      logger,
    })
  })

  test('creates a new connection with the seed attributes', () => {
    const connection = builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toEqual(host)
    expect(connection.port).toEqual(port)
    expect(connection.ssl).toEqual(ssl)
    expect(connection.sasl).toEqual(sasl)
    expect(connection.clientId).toEqual(clientId)
    expect(connection.connectionTimeout).toEqual(connectionTimeout)
    expect(connection.retry).toEqual(retry)
    expect(connection.logger).toEqual(logger)
  })

  test('accepts overrides for host, port and rack', () => {
    const connection = builder.build({
      host: 'host.another',
      port: 8888,
      rack: 'rack',
    })
    expect(connection.host).toEqual('host.another')
    expect(connection.port).toEqual(8888)
    expect(connection.rack).toEqual('rack')
  })
})
