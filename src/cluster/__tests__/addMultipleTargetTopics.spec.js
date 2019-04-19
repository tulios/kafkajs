const { createCluster, secureRandom, createTopic } = require('testHelpers')

describe('Cluster > addMultipleTargetTopics', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    cluster.brokerPool.metadata = { some: 'metadata' }
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('add multiple topics to the target list', async () => {
    const topic1 = `topic-${secureRandom()}`
    const topic2 = `topic-${secureRandom()}`
    await createTopic({ topic: topic1 })
    await createTopic({ topic: topic2 })
    expect(Array.from(cluster.targetTopics)).toEqual([])

    await cluster.addMultipleTargetTopics([topic1, topic2])
    expect(Array.from(cluster.targetTopics)).toEqual([topic1, topic2])
  })

  test('refresh metadata if the list of topics has changed', async () => {
    cluster.refreshMetadata = jest.fn()
    const topic1 = `topic-${secureRandom()}`
    await cluster.addMultipleTargetTopics([topic1])
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('refresh metadata if no metadata was loaded before', async () => {
    cluster.refreshMetadata = jest.fn()
    const topic1 = `topic-${secureRandom()}`
    await cluster.addMultipleTargetTopics([topic1])
    await cluster.addMultipleTargetTopics([topic1])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)

    cluster.brokerPool.metadata = null
    await cluster.addMultipleTargetTopics([topic1])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
  })
})
