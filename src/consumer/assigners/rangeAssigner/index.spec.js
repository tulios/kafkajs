const RoundRobinAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

describe('Consumer > assigners > RoundRobinAssigner', () => {
  let cluster, topics, metadata, assigner

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: topic => metadata[topic] }
    assigner = RoundRobinAssigner({ cluster })
  })

  describe('#assign', () => {
    test('java example topic-partitions assignment', async () => {
      topics = ['t0', 't1']
      metadata['t0'] = Array(3)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['t1'] = Array(3)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [{ memberId: 'C0' }, { memberId: 'C1' }]

      const assignment = await assigner.assign({ members, topics })

      expect(assignment).toEqual([
        {
          memberId: 'C0',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              t0: [0, 1],
              t1: [0, 1],
            },
          }),
        },
        {
          memberId: 'C1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              t0: [2],
              t1: [2],
            },
          }),
        },
      ])
    })

    test('more complex topic-partitions assignment', async () => {
      topics = ['topic-A', 'topic-B', 'topic-C']

      metadata['topic-A'] = Array(12)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['topic-B'] = Array(12)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['topic-C'] = Array(3)
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
              'topic-A': [0, 1, 2],
              'topic-B': [0, 1, 2],
              'topic-C': [0],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [3, 4, 5],
              'topic-B': [3, 4, 5],
              'topic-C': [1],
            },
          }),
        },
        {
          memberId: 'member-3',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [6, 7, 8],
              'topic-B': [6, 7, 8],
              'topic-C': [2],
            },
          }),
        },
        {
          memberId: 'member-4',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [9, 10, 11],
              'topic-B': [9, 10, 11],
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
