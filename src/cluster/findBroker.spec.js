const { createCluster } = require('../../testHelpers')

describe('Cluster > findBroker', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('find a broker by nodeId', async () => {
    await cluster.refreshMetadata()
    const nodeId = Object.keys(cluster.brokerPool)[0]

    const broker = await cluster.findBroker({ nodeId })
    expect(broker).toEqual(cluster.brokerPool[nodeId])
  })

  test('connect the broker if it is not connected', async () => {
    await cluster.refreshMetadata()
    const nodeId = Object.keys(cluster.brokerPool).find(id => !cluster.brokerPool[id].isConnected())
    expect(cluster.brokerPool[nodeId].isConnected()).toEqual(false)

    const broker = await cluster.findBroker({ nodeId })
    expect(broker.isConnected()).toEqual(true)
  })
})
