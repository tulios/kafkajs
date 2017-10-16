const { createCluster } = require('../../testHelpers')
const Broker = require('../broker')

describe('Cluster > refreshMetadata', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('update metadata and broker pool', async () => {
    expect(cluster.metadata).toEqual(null)
    expect(cluster.brokerPool).toEqual({})

    await cluster.refreshMetadata()
    expect(cluster.metadata).not.toEqual(null)
    expect(cluster.brokerPool).not.toEqual({})
  })

  test('query metadata for the target topics', async () => {
    cluster.seedBroker.metadata = jest.fn(() => ({ brokers: [] }))
    cluster.targetTopics.add('test-topic')
    await cluster.refreshMetadata()

    expect(cluster.seedBroker.metadata).toHaveBeenCalledWith(Array.from(cluster.targetTopics))
  })

  test('create the broker pool by nodeId and instances of Broker', async () => {
    cluster.seedBroker.metadata = jest.fn(() => ({
      brokers: [
        { nodeId: 'A', host: 'A', port: 'A', rack: 'A' },
        { nodeId: 'B', host: 'B', port: 'B', rack: 'B' },
      ],
    }))

    await cluster.refreshMetadata()
    expect(cluster.brokerPool).toEqual({
      A: expect.any(Broker),
      B: expect.any(Broker),
    })

    const a = cluster.brokerPool['A'].connection
    expect(a.host).toEqual('A')
    expect(a.port).toEqual('A')
    expect(a.rack).toEqual('A')

    const b = cluster.brokerPool['B'].connection
    expect(b.host).toEqual('B')
    expect(b.port).toEqual('B')
    expect(b.rack).toEqual('B')
  })

  test('include the seed broker into the broker pool', async () => {
    await cluster.refreshMetadata()
    const seed = cluster.seedBroker.connection
    const brokers = Object.values(cluster.brokerPool)
    const seedFromBrokerPool = brokers
      .map(b => b.connection)
      .find(b => b.host === seed.host && b.port === seed.port)

    expect(seedFromBrokerPool).toEqual(seed)
  })
})
