const { newLogger } = require('testHelpers')
const connectionBuilder = require('../connectionBuilder')
const Connection = require('../../network/connection')

describe('Cluster > ConnectionBuilder', () => {
  let builder

  const brokers = ['host.test:7777', 'host2.test:7778', 'host3.test:7779']
  const ssl = { ssl: true }
  const sasl = { sasl: true }
  const clientId = 'test-client-id'
  const connectionTimeout = 30000
  const retry = { retry: true }
  const logger = newLogger()
  const socketFactory = jest.fn()

  beforeEach(() => {
    builder = connectionBuilder({
      socketFactory,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
      logger,
    })
  })

  test('creates a new connection using the first the seed broker', () => {
    const connection = builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toEqual('host.test')
    expect(connection.port).toEqual(7777)
    expect(connection.ssl).toEqual(ssl)
    expect(connection.sasl).toEqual(sasl)
    expect(connection.clientId).toEqual(clientId)
    expect(connection.connectionTimeout).toEqual(connectionTimeout)
    expect(connection.retry).toEqual(retry)
    expect(connection.logger).not.toBeFalsy()
    expect(connection.socketFactory).toBe(socketFactory)
  })

  test('when called without host and port iterates throught the seed brokers', () => {
    expect(builder.build()).toEqual(expect.objectContaining({ host: 'host.test', port: 7777 }))
    expect(builder.build()).toEqual(expect.objectContaining({ host: 'host2.test', port: 7778 }))
    expect(builder.build()).toEqual(expect.objectContaining({ host: 'host3.test', port: 7779 }))
    expect(builder.build()).toEqual(expect.objectContaining({ host: 'host.test', port: 7777 }))
    expect(builder.build()).toEqual(expect.objectContaining({ host: 'host2.test', port: 7778 }))
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
