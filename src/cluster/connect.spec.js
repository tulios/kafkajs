const { createCluster } = require('../../testHelpers')

describe('Cluster > connect', () => {
  let cluster

  beforeEach(() => {
    cluster = createCluster()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('connect the seed broker', async () => {
    expect(cluster.seedBroker.isConnected()).toEqual(false)
    await cluster.connect()
    expect(cluster.seedBroker.isConnected()).toEqual(true)
  })

  test('load the versions from the seed broker', async () => {
    expect(cluster.versions).toEqual(null)
    await cluster.connect()
    expect(cluster.versions).toEqual(cluster.seedBroker.versions)
  })
})
