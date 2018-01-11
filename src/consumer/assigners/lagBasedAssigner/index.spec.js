const createLagBasedAssigner = require('./index')
const { newLogger } = require('testHelpers')

describe('Consumer > assigners > LagBasedAssigner', async () => {
  let cluster, topics, metadata, assignPartitions

  beforeEach(() => {
    metadata = {}
    cluster = {
      findTopicPartitionMetadata: topic => metadata[topic],
      fetchTopicsOffset: async topics =>
        topics.filter(({ topic }) => metadata[topic]).map(({ topic, partitions }) => ({
          topic,
          partitions: partitions
            .filter(({ partition }) =>
              metadata[topic].map(({ partitionId }) => partitionId).includes(partition)
            )
            .map(({ partition }) => ({
              partition,
              offset: `${parseInt(partition) * 2}`,
            })),
        })),
      fetchGroupOffset: async ({ groupId, topicsWithPartitions }) =>
        topicsWithPartitions
          .filter(({ topic }) => metadata[topic])
          .map(({ topic, partitions }) => ({
            topic,
            partitions: partitions
              .filter(({ partition }) =>
                metadata[topic].map(({ partitionId }) => partitionId).includes(partition)
              )
              .map(({ partition }) => ({
                partition,
                offset: `${parseInt(partition)}`,
              })),
          })),
    }
    assignPartitions = createLagBasedAssigner({
      cluster,
      groupId: 'test-topic-group-id',
      logger: newLogger(),
    })
  })

  test('assign partitions evenly based on offset lag', async () => {
    topics = ['topic-A', 'topic-B']
    metadata['topic-A'] = Array(14)
      .fill()
      .map((_, i) => ({ partitionId: i }))

    metadata['topic-B'] = Array(5)
      .fill()
      .map((_, i) => ({ partitionId: i }))

    const members = [
      { memberId: 'member-3' },
      { memberId: 'member-1' },
      { memberId: 'member-4' },
      { memberId: 'member-2' },
    ]

    await expect(assignPartitions({ members, topics })).resolves.toEqual([
      {
        memberId: 'member-4',
        memberAssignment: {
          'topic-A': [10, 9, 2, 1],
        },
      },
      {
        memberId: 'member-3',
        memberAssignment: {
          'topic-A': [11, 8, 3, 0],
          'topic-B': [0],
        },
      },
      {
        memberId: 'member-2',
        memberAssignment: {
          'topic-A': [12, 7, 4],
          'topic-B': [4, 1],
        },
      },
      {
        memberId: 'member-1',
        memberAssignment: {
          'topic-A': [13, 6, 5],
          'topic-B': [3, 2],
        },
      },
    ])
  })
})
