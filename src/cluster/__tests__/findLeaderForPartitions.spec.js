const { createCluster, secureRandom } = require('testHelpers')

describe('Cluster > findLeaderForPartitions', () => {
  let cluster, topic

  beforeEach(() => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()
  })

  test('returns the partition metadata of a topic', async () => {
    cluster.brokerPool.metadata = {
      topicMetadata: [
        {
          topic,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 2,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 0, 1],
            },
            {
              partitionErrorCode: 0,
              partitionId: 5,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 1, 0],
            },
            {
              partitionErrorCode: 0,
              partitionId: 4,
              leader: 1,
              replicas: [0, 1, 2],
              isr: [1, 0, 2],
            },
            {
              partitionErrorCode: 0,
              partitionId: 1,
              leader: 1,
              replicas: [0, 1, 2],
              isr: [1, 2, 0],
            },
            {
              partitionErrorCode: 0,
              partitionId: 3,
              leader: 0,
              replicas: [0, 1, 2],
              isr: [0, 2, 1],
            },
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 0,
              replicas: [0, 1, 2],
              isr: [0, 1, 2],
            },
          ],
        },
      ],
    }

    const partitions = [0, 5]
    expect(cluster.findLeaderForPartitions(topic, partitions)).toEqual({ '0': [0], '2': [5] })
  })

  it('does not include leaders for topics without metadata', () => {
    cluster.brokerPool.metadata = {
      topicMetadata: [
        {
          topic,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 2,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 0, 1],
            },
          ],
        },
      ],
    }

    const anotherTopic = `test-topic-${secureRandom()}`
    const partitions = [0]
    expect(cluster.findLeaderForPartitions(anotherTopic, partitions)).toEqual({})
  })
})
