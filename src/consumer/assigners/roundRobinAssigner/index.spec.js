const RoundRobinAssigner = require('./index')

describe('Consumer > assigners > RoundRobinAssigner', () => {
  let cluster, topics, metadata, assigner

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: topic => metadata[topic] }
    assigner = RoundRobinAssigner({ cluster })
    topics = ['topic-A', 'topic-B']
  })

  describe('#assign', () => {
    test('assign all partitions evenly', () => {
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

      expect(assigner.assign({ members, topics })).toEqual([
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

  describe('#protocol', () => {
    test('returns the assigner name and metadata', () => {
      expect(assigner.protocol({ topics })).toEqual({
        name: assigner.name,
        metadata: '{"version":1,"topics":["topic-A","topic-B"]}',
      })
    })
  })
})
