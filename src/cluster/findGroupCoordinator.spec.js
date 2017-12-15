jest.mock('../utils/shuffle')
const shuffle = require('../utils/shuffle')
shuffle.mockImplementation(brokers => brokers.sort((a, b) => a > b))
const { createCluster, secureRandom } = require('../../testHelpers')
const { KafkaJSBrokerNotFound, KafkaJSConnectionError } = require('../errors')

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
      .mockImplementationOnce(() => firstNode)

    await expect(cluster.findGroupCoordinator({ groupId })).resolves.toEqual(firstNode)
    expect(cluster.findBroker).toHaveBeenCalledTimes(3)
  })

  test('attempt to find coordinator across all brokers until one is found', async () => {
    jest.spyOn(cluster.brokerPool[0], 'findGroupCoordinator').mockImplementation(() => {
      throw new KafkaJSConnectionError('Something went wrong')
    })
    jest.spyOn(cluster.brokerPool[1], 'findGroupCoordinator').mockImplementation(() => {
      throw new KafkaJSConnectionError('Something went wrong')
    })
    jest
      .spyOn(cluster.brokerPool[2], 'findGroupCoordinator')
      .mockImplementation(() => ({ coordinator: { nodeId: 2 } }))

    await expect(cluster.findGroupCoordinator({ groupId })).resolves.toEqual(cluster.brokerPool[2])
  })
})
