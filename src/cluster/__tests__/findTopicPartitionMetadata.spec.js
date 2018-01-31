const { createCluster, secureRandom } = require('testHelpers')

describe('Cluster > findTopicPartitionMetadata', () => {
  let cluster, topic

  beforeEach(() => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()
  })

  test('returns the partition metadata of a topic', () => {
    const partitionMetadata = {
      isr: [2],
      leader: 2,
      partitionErrorCode: 0,
      partitionId: 0,
      replicas: [2],
    }
    cluster.brokerPool.metadata = { topicMetadata: [{ topic, partitionMetadata }] }
    expect(cluster.findTopicPartitionMetadata(topic)).toEqual(partitionMetadata)
  })

  test('throws and error if the topicMetadata is not loaded', () => {
    cluster.brokerPool.metadata = null
    expect(() => cluster.findTopicPartitionMetadata(topic)).toThrowError(
      /Topic metadata not loaded/
    )

    cluster.brokerPool.metadata = {}
    expect(() => cluster.findTopicPartitionMetadata(topic)).toThrowError(
      /Topic metadata not loaded/
    )
  })
})
