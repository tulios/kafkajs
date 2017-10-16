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

  describe('#isConnected', () => {
    test('returns false when the seed broker is not connected', () => {
      cluster.seedBroker.isConnected = jest.fn(() => false)
      expect(cluster.isConnected()).toEqual(false)
    })

    test('returns true when the seed broker is connected', async () => {
      cluster.seedBroker.isConnected = jest.fn(() => true)
      expect(cluster.isConnected()).toEqual(true)
    })
  })
})
