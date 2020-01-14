const { newLogger } = require('testHelpers')
const connectionBuilder = require('../connectionBuilder')
const Connection = require('../../network/connection')
const { KafkaJSNonRetriableError } = require('../../errors')

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

  test('creates a new connection using a random broker', () => {
    const connection = builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toBeOneOf(['host.test', 'host2.test', 'host3.test'])
    expect(connection.port).toBeOneOf([7777, 7778, 7779])
    expect(connection.ssl).toEqual(ssl)
    expect(connection.sasl).toEqual(sasl)
    expect(connection.clientId).toEqual(clientId)
    expect(connection.connectionTimeout).toEqual(connectionTimeout)
    expect(connection.retry).toEqual(retry)
    expect(connection.logger).not.toBeFalsy()
    expect(connection.socketFactory).toBe(socketFactory)
  })

  test('when called without host and port iterates throught the seed brokers', () => {
    const connections = Array(brokers.length)
      .fill()
      .map(() => {
        const { host, port } = builder.build()
        return `${host}:${port}`
      })
    expect(connections).toIncludeSameMembers(brokers)
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

  it('throws an exception if brokers list is empty', () => {
    expect(() => {
      builder = connectionBuilder({
        socketFactory,
        brokers: [],
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        retry,
        logger,
      })
    }).toThrow(
      KafkaJSNonRetriableError,
      'Failed to connect: expected brokers array and got nothing'
    )
  })

  it('throws an exception if brokers is null', () => {
    expect(() => {
      builder = connectionBuilder({
        socketFactory,
        brokers: null,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        retry,
        logger,
      })
    }).toThrow(
      KafkaJSNonRetriableError,
      'Failed to connect: expected brokers array and got nothing'
    )
  })
})
