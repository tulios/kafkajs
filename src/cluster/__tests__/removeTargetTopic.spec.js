const { createCluster, secureRandom, createTopic } = require('testHelpers')

describe('Cluster > removeTargetTopic', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    cluster.brokerPool.metadata = { some: 'metadata' }
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('remove the topic from the target list', async () => {
    const topic1 = `topic-${secureRandom()}`
    const topic2 = `topic-${secureRandom()}`
    await createTopic({ topic: topic1 })
    await createTopic({ topic: topic2 })
    await cluster.addMultipleTargetTopics([topic1, topic2])
    expect(Array.from(cluster.targetTopics)).toEqual([topic1, topic2])

    await cluster.removeTargetTopic(topic1)
    expect(Array.from(cluster.targetTopics)).toEqual([topic2])

    await cluster.removeTargetTopic(topic2)
    expect(Array.from(cluster.targetTopics)).toEqual([])
  })
})
