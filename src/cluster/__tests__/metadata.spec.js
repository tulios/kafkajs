const { createCluster, secureRandom, createTopic } = require('testHelpers')

describe('Cluster > metadata', () => {
  let cluster, topic1, topic2, topic3

  beforeEach(async () => {
    topic1 = `test-topic-${secureRandom()}`
    topic2 = `test-topic-${secureRandom()}`
    topic3 = `test-topic-${secureRandom()}`

    await createTopic({ topic: topic1 })
    await createTopic({ topic: topic2 })
    await createTopic({ topic: topic3 })

    cluster = createCluster()
    await cluster.connect()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('returns metadata for a set of topics', async () => {
    const response = await cluster.metadata({ topics: [topic1, topic2] })
    expect(response.topicMetadata.length).toEqual(2)

    const topics = response.topicMetadata.map(({ topic }) => topic).sort()
    expect(topics).toEqual([topic1, topic2].sort())
  })

  test('returns metadata for all topics', async () => {
    const response = await cluster.metadata({ topics: [] })
    expect(response.topicMetadata.length).toBeGreaterThanOrEqual(3)

    const topics = response.topicMetadata.map(({ topic }) => topic).sort()
    expect(topics).toEqual(expect.arrayContaining([topic1, topic2, topic3].sort()))
  })
})
