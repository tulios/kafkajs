const { createCluster, createConnection, newLogger } = require('../../testHelpers')
const Broker = require('../broker')
const { KafkaJSConnectionError } = require('../errors')

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

  test('recreates the connection on connection errors', async () => {
    await cluster.refreshMetadata()
    const nodeId = 'fakebroker'
    const mockBroker = new Broker(createConnection(), newLogger())
    jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
      throw new KafkaJSConnectionError('Connection lost')
    })
    cluster.brokerPool[nodeId] = mockBroker

    const broker = await cluster.findBroker({ nodeId })
    expect(broker.isConnected()).toEqual(true)
  })

  test('throws an error when the broker is not found', async () => {
    await expect(cluster.findBroker({ nodeId: 627 })).rejects.toHaveProperty(
      'message',
      'Broker 627 not found in the cached metadata'
    )
  })
})
