const { createCluster } = require('testHelpers')
const { KafkaJSMetadataNotLoaded, KafkaJSBrokerNotFound } = require('../../errors')

describe('Cluster > findControllerBroker', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    cluster.brokerPool.metadata = { controllerId: 'controllerNodeId' }
    cluster.findBroker = jest.fn()
  })

  test('finds the broker of the controller', async () => {
    cluster.findBroker.mockImplementationOnce(() => true)
    await expect(cluster.findControllerBroker()).resolves.toEqual(true)
    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: 'controllerNodeId' })
  })

  test('throws KafkaJSTopicMetadataNotLoaded if metadata is not loaded', async () => {
    cluster.brokerPool.metadata = null
    await expect(cluster.findControllerBroker()).rejects.toThrow(KafkaJSMetadataNotLoaded)
  })

  test('throws KafkaJSBrokerNotFound if the node is not in the cache', async () => {
    cluster.findBroker.mockImplementationOnce(() => {
      throw new KafkaJSBrokerNotFound('not found')
    })

    await expect(cluster.findControllerBroker()).rejects.toThrow(KafkaJSBrokerNotFound)
  })
})
