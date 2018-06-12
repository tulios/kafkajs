const { createCluster, secureRandom, createTopic } = require('testHelpers')

describe('Cluster > addTargetTopic', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    cluster.brokerPool.metadata = { some: 'metadata' }
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('add the new topic to the target list', async () => {
    const topic1 = `topic-${secureRandom()}`
    const topic2 = `topic-${secureRandom()}`
    await createTopic({ topic: topic1 })
    await createTopic({ topic: topic2 })
    expect(Array.from(cluster.targetTopics)).toEqual([])

    await cluster.addTargetTopic(topic1)
    await cluster.addTargetTopic(topic1)
    expect(Array.from(cluster.targetTopics)).toEqual([topic1])

    await cluster.addTargetTopic(topic2)
    expect(Array.from(cluster.targetTopics)).toEqual([topic1, topic2])
  })

  test('refresh metadata if the list of topics has changed', async () => {
    cluster.refreshMetadata = jest.fn()
    const topic1 = `topic-${secureRandom()}`
    await cluster.addTargetTopic(topic1)
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('refresh metadata if no metadata was loaded before', async () => {
    cluster.refreshMetadata = jest.fn()
    const topic1 = `topic-${secureRandom()}`
    await cluster.addTargetTopic(topic1)
    await cluster.addTargetTopic(topic1)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)

    cluster.brokerPool.metadata = null
    await cluster.addTargetTopic(topic1)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
  })
})
