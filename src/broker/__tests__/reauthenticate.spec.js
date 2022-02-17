const {
  saslOAuthBearerConnectionOpts,
  newLogger,
  describeIfOauthbearerEnabled,
  createConnectionPool,
} = require('testHelpers')

const { SaslAuthenticate: apiSaslAuthenticateKey } = require('../../protocol/requests/apiKeys')
const Broker = require('../index')

/**
 * Session lifetime on broker is configured in docker-compose.2_4_oauthbearer.yml with 15000ms in env KAFKA_CONNECTIONS_MAX_REAUTH_MS
 * see https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_connections.max.reauth.ms
 */
describeIfOauthbearerEnabled('Brokers re-authentication with SASL OAUTHBEARER', () => {
  let broker

  const wait = (time = 1000) =>
    new Promise(resolve => {
      setTimeout(resolve, time)
    })

  const getAuthCalls = spy =>
    spy.mock.calls.filter(mock => mock[0].request.apiKey === apiSaslAuthenticateKey)

  test('Does not re-authenticate when still within session validity threshold', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 2000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(1)
  })

  test('Re-authenticate if needed before making a request', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 14900,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(2)
  })

  test('Re-authenticate only once with multiple parallel requests', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 14900,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await Promise.all([
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
    ])

    expect(getAuthCalls(spy).length).toBe(2)
  })

  test('Re-authenticate every request sent when re-authentication threshold is same as session lifetime', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 15000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(4)
  })

  test('Re-authenticates only once with multiple requests sent in parallel when re-authentication threshold is same as session lifetime', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 15000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await Promise.all([
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
    ])

    expect(getAuthCalls(spy).length).toBe(2)
  })
})
