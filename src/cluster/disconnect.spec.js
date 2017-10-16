const { createCluster } = require('../../testHelpers')

describe('Cluster > connect', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('disconnect the seed broker', async () => {
    expect(cluster.seedBroker.isConnected()).toEqual(true)
    await cluster.disconnect()
    expect(cluster.seedBroker.isConnected()).toEqual(false)
  })

  test('disconnect all brokers in the broker pool', async () => {
    await cluster.refreshMetadata()

    const brokerPool = cluster.brokerPool
    const brokers = Object.values(brokerPool)

    expect(brokers.length).toBeGreaterThan(0)
    await Promise.all(
      brokers.map(async b => {
        await b.connect()
      })
    )

    expect(brokers.map(b => b.isConnected())).toEqual(Array(brokers.length).fill(true))
    await cluster.disconnect()

    expect(brokers.length).toBeGreaterThan(0)
    expect(brokers.map(b => b.isConnected())).toEqual(Array(brokers.length).fill(false))
  })

  test('erase metadata and broker pool information', async () => {
    await cluster.refreshMetadata()
    expect(cluster.metadata).not.toEqual(null)
    expect(cluster.brokerPool).not.toEqual({})

    await cluster.disconnect()
    expect(cluster.metadata).toEqual(null)
    expect(cluster.brokerPool).toEqual({})
  })
})
