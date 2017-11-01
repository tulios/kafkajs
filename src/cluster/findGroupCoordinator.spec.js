const { createCluster, secureRandom } = require('../../testHelpers')
const { KafkaJSBrokerNotFound } = require('../errors')

describe('Cluster > findGroupCoordinator', () => {
  let cluster, groupId

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    await cluster.refreshMetadata()
    groupId = `test-group-${secureRandom()}`
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('find the group coordinator', async () => {
    const broker = await cluster.findGroupCoordinator({ groupId })
    expect(broker).not.toBeFalsy()
  })

  test('refresh the metadata and try again in case of broker not found', async () => {
    const firstNodeId = Object.keys(cluster.brokerPool)[0]
    const firstNode = cluster.brokerPool[firstNodeId]

    cluster.findBroker = jest
      .fn()
      .mockImplementationOnce(() => {
        throw new KafkaJSBrokerNotFound('Not found')
      })
      .mockImplementationOnce(() => firstNode)

    await expect(cluster.findGroupCoordinator({ groupId })).resolves.toEqual(firstNode)
    expect(cluster.findBroker).toHaveBeenCalledTimes(2)
  })
})
