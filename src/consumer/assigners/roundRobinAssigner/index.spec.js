const RoundRobinAssigner = require('./index')

describe('Consumer > assigners > RoundRobinAssigner', () => {
  let cluster, topics, metadata, assignPartitions

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: topic => metadata[topic] }
    assignPartitions = RoundRobinAssigner({ cluster })
  })

  test('assign all partitions evenly', () => {
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

    expect(assignPartitions({ members, topics })).toEqual([
      {
        memberId: 'member-1',
        memberAssignment: {
          'topic-A': [0, 4, 8, 12],
          'topic-B': [0, 4],
        },
      },
      {
        memberId: 'member-2',
        memberAssignment: {
          'topic-A': [1, 5, 9, 13],
          'topic-B': [1],
        },
      },
      {
        memberId: 'member-3',
        memberAssignment: {
          'topic-A': [2, 6, 10],
          'topic-B': [2],
        },
      },
      {
        memberId: 'member-4',
        memberAssignment: {
          'topic-A': [3, 7, 11],
          'topic-B': [3],
        },
      },
    ])
  })
})
