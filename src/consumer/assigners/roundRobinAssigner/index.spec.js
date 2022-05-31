const RoundRobinAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

describe('Consumer > assigners > RoundRobinAssigner', () => {
  let cluster, topics, metadata, assigner

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: topic => metadata[topic] }
    assigner = RoundRobinAssigner({ cluster })
    topics = ['topic-A', 'topic-B']
  })

  describe('#assign', () => {
    test('assign all topic-partitions evenly', async () => {
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

      const assignment = await assigner.assign({ members, topics })

      expect(assignment).toEqual([
        {
          memberId: 'member-1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [0, 4, 8, 12],
              'topic-B': [2],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [1, 5, 9, 13],
              'topic-B': [3],
            },
          }),
        },
        {
          memberId: 'member-3',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [2, 6, 10],
              'topic-B': [0, 4],
            },
          }),
        },
        {
          memberId: 'member-4',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [3, 7, 11],
              'topic-B': [1],
            },
          }),
        },
      ])
    })

    test('assign topics with names taken from builtin functions', async () => {
      topics = ['shift', 'toString']
      metadata['shift'] = [{ partitionId: 0 }]
      metadata['toString'] = [{ partitionId: 0 }]
      const members = [{ memberId: 'member-1' }]

      const assignment = await assigner.assign({ members, topics })

      expect(assignment).toEqual([
        {
          memberId: 'member-1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              shift: [0],
              toString: [0],
            },
          }),
        },
      ])
    })
  })

  describe('#protocol', () => {
    test('returns the assigner name and metadata', () => {
      expect(assigner.protocol({ topics })).toEqual({
        name: assigner.name,
        metadata: MemberMetadata.encode({ version: assigner.version, topics }),
      })
    })
  })
})
