const { createCluster } = require('testHelpers')

describe('Cluster > clusterId', () => {
  let cluster

  beforeEach(() => {
    cluster = createCluster()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('returns null before metadata has been fetched', () => {
    expect(cluster.clusterId()).toBeNull()
  })

  test('returns a non-empty string after connecting and fetching metadata', async () => {
    await cluster.connect()
    await cluster.refreshMetadata()
    const id = cluster.clusterId()
    expect(id).toBeTruthy()
    expect(typeof id).toBe('string')
  })
})
