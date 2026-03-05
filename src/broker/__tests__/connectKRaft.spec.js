const {
  createConnectionPool,
  connectionOpts,
  newLogger,
  testIfKafkaAtLeast_4_0_0,
} = require('testHelpers')

const Broker = require('../index')

describe('Broker > connect (KRaft)', () => {
  let broker, connectionPool

  beforeEach(() => {
    connectionPool = createConnectionPool(connectionOpts())
    broker = new Broker({ connectionPool, logger: newLogger() })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  testIfKafkaAtLeast_4_0_0('connects to a KRaft-mode broker', async () => {
    await broker.connect()
    expect(broker.connectionPool.isConnected()).toEqual(true)
    expect(broker.versions).toBeTruthy()
  })

  testIfKafkaAtLeast_4_0_0('negotiates API versions with KRaft broker', async () => {
    await broker.connect()

    // KRaft brokers should support these core APIs
    const versions = broker.versions
    expect(versions).toBeTruthy()

    // Produce API (key 0) should be supported
    expect(versions[0]).toBeTruthy()
    expect(versions[0].minVersion).toBeDefined()
    expect(versions[0].maxVersion).toBeDefined()

    // Fetch API (key 1) should be supported
    expect(versions[1]).toBeTruthy()

    // Metadata API (key 3) should be supported
    expect(versions[3]).toBeTruthy()

    // ApiVersions API (key 18) should be supported
    expect(versions[18]).toBeTruthy()
  })

  testIfKafkaAtLeast_4_0_0('KRaft broker does not require ZooKeeper', async () => {
    // This test simply verifies the broker starts and connects without ZooKeeper
    await broker.connect()
    expect(broker.connectionPool.isConnected()).toEqual(true)

    // Verify we can perform basic metadata operations
    const metadata = await broker.metadata([])
    expect(metadata).toBeTruthy()
    expect(metadata.brokers).toBeTruthy()
    expect(metadata.brokers.length).toBeGreaterThan(0)
  })
})
