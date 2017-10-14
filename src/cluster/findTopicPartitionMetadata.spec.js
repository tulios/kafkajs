const { createCluster, secureRandom } = require('../../testHelpers')

describe('Cluster > findTopicPartitionMetadata', () => {
  let cluster, topic

  beforeEach(() => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()
  })

  test('returns the partition metadata of a topic', async () => {
    const partitionMetadata = {
      isr: [2],
      leader: 2,
      partitionErrorCode: 0,
      partitionId: 0,
      replicas: [2],
    }
    cluster.metadata.topicMetadata = [{ topic, partitionMetadata }]
    expect(cluster.findTopicPartitionMetadata(topic)).toEqual(partitionMetadata)
  })
})
