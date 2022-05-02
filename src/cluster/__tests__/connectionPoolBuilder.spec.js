const { newLogger } = require('testHelpers')
const connectionPoolBuilder = require('../connectionPoolBuilder')
const ConnectionPool = require('../../network/connectionPool')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../../errors')

describe('Cluster > ConnectionBuilder', () => {
  let builder

  const brokers = ['host.test:7777', 'host2.test:7778', 'host3.test:7779']
  const ssl = { ssl: true }
  const sasl = { sasl: true }
  const clientId = 'test-client-id'
  const connectionTimeout = 30000
  const logger = newLogger()
  const socketFactory = jest.fn()

  beforeEach(() => {
    builder = connectionPoolBuilder({
      socketFactory,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      logger,
    })
  })

  test('creates a new connection using a random broker', async () => {
    const connectionPool = await builder.build()
    expect(connectionPool).toBeInstanceOf(ConnectionPool)
    expect(connectionPool.host).toBeOneOf(['host.test', 'host2.test', 'host3.test'])
    expect(connectionPool.port).toBeOneOf([7777, 7778, 7779])
    expect(connectionPool.ssl).toEqual(ssl)
    expect(connectionPool.sasl).toEqual(sasl)
    expect(connectionPool.clientId).toEqual(clientId)
    expect(connectionPool.connectionTimeout).toEqual(connectionTimeout)
    expect(connectionPool.logger).not.toBeFalsy()
    expect(connectionPool.socketFactory).toBe(socketFactory)
  })

  test('when called without host and port iterates throught the seed brokers', async () => {
    const connections = []
    for (let i = 0; i < brokers.length; i++) {
      const { host, port } = await builder.build()
      connections.push(`${host}:${port}`)
    }

    expect(connections).toIncludeSameMembers(brokers)
  })

  test('accepts overrides for host, port and rack', async () => {
    const connectionPool = await builder.build({
      host: 'host.another',
      port: 8888,
      rack: 'rack',
    })
    expect(connectionPool.host).toEqual('host.another')
    expect(connectionPool.port).toEqual(8888)
    expect(connectionPool.rack).toEqual('rack')
  })

  it('throws an exception if brokers list is empty', async () => {
    await expect(
      connectionPoolBuilder({
        socketFactory,
        brokers: [],
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(new KafkaJSNonRetriableError('Failed to connect: brokers array is empty'))
  })

  it('throws an exception if brokers is null', async () => {
    await expect(
      connectionPoolBuilder({
        socketFactory,
        brokers: null,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(new KafkaJSNonRetriableError('Failed to connect: brokers should not be null'))
  })

  it('throws an exception if one of the brokers is not a string', async () => {
    await expect(
      connectionPoolBuilder({
        socketFactory,
        brokers: ['localhost:1234', undefined],
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(
      new KafkaJSNonRetriableError('Failed to connect: broker at index 1 is invalid "undefined"')
    )
  })

  it('throws an KafkaJSConnectionError if brokers is function and returning null', async () => {
    await expect(
      connectionPoolBuilder({
        socketFactory,
        brokers: () => null,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(new KafkaJSConnectionError('Failed to connect: brokers should not be null'))
  })

  it('throws an KafkaJSConnectionError if brokers is function crashes', async () => {
    await expect(
      connectionPoolBuilder({
        socketFactory,
        brokers: () => {
          throw new Error('oh a crash!')
        },
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(
      new KafkaJSConnectionError('Failed to connect: "config.brokers" threw: oh a crash!')
    )
  })

  it('brokers can be function that returns array of host:port strings', async () => {
    const builder = connectionPoolBuilder({
      socketFactory,
      brokers: () => ['host.test:7777'],
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      logger,
    })

    const connectionPool = await builder.build()
    expect(connectionPool).toBeInstanceOf(ConnectionPool)
    expect(connectionPool.host).toBe('host.test')
    expect(connectionPool.port).toBe(7777)
  })

  it('brokers can be async function that returns array of host:port strings', async () => {
    const builder = connectionPoolBuilder({
      socketFactory,
      brokers: async () => ['host.test:7777'],
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      logger,
    })

    const connectionPool = await builder.build()
    expect(connectionPool).toBeInstanceOf(ConnectionPool)
    expect(connectionPool.host).toBe('host.test')
    expect(connectionPool.port).toBe(7777)
  })
})
