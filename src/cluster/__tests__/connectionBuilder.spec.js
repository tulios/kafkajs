const { newLogger } = require('testHelpers')
const connectionBuilder = require('../connectionBuilder')
const Connection = require('../../network/connection')
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
    builder = connectionBuilder({
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
    const connection = await builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toBeOneOf(['host.test', 'host2.test', 'host3.test'])
    expect(connection.port).toBeOneOf([7777, 7778, 7779])
    expect(connection.ssl).toEqual(ssl)
    expect(connection.sasl).toEqual(sasl)
    expect(connection.clientId).toEqual(clientId)
    expect(connection.connectionTimeout).toEqual(connectionTimeout)
    expect(connection.logger).not.toBeFalsy()
    expect(connection.socketFactory).toBe(socketFactory)
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
    const connection = await builder.build({
      host: 'host.another',
      port: 8888,
      rack: 'rack',
    })
    expect(connection.host).toEqual('host.another')
    expect(connection.port).toEqual(8888)
    expect(connection.rack).toEqual('rack')
  })

  it('throws an exception if brokers list is empty', async () => {
    await expect(
      connectionBuilder({
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
      connectionBuilder({
        socketFactory,
        brokers: null,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(
      new KafkaJSNonRetriableError('Failed to connect: brokers parameter should not be null')
    )
  })

  it('throws an KafkaJSConnectionError if brokers is function and returning null', async () => {
    await expect(
      connectionBuilder({
        socketFactory,
        brokers: () => null,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        logger,
      }).build()
    ).rejects.toEqual(
      new KafkaJSConnectionError('Failed to connect: "config.brokers" returned void or empty array')
    )
  })

  it('throws an KafkaJSConnectionError if brokers is function crashes', async () => {
    await expect(
      connectionBuilder({
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
    const builder = connectionBuilder({
      socketFactory,
      brokers: () => ['host.test:7777'],
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      logger,
    })

    const connection = await builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toBe('host.test')
    expect(connection.port).toBe(7777)
  })

  it('brokers can be async function that returns array of host:port strings', async () => {
    const builder = connectionBuilder({
      socketFactory,
      brokers: async () => ['host.test:7777'],
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      logger,
    })

    const connection = await builder.build()
    expect(connection).toBeInstanceOf(Connection)
    expect(connection.host).toBe('host.test')
    expect(connection.port).toBe(7777)
  })
})
