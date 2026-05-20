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
              'topic-B': [0, 4],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [1, 5, 9, 13],
              'topic-B': [1],
            },
          }),
        },
        {
          memberId: 'member-3',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [2, 6, 10],
              'topic-B': [2],
            },
          }),
        },
        {
          memberId: 'member-4',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [3, 7, 11],
              'topic-B': [3],
            },
          }),
        },
      ])
    })

    test('assign topic-partitions only to members subscribed to that topic', async () => {
      metadata['topic-A'] = Array(4)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['topic-B'] = Array(3)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['topic-C'] = Array(2)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [
        {
          memberId: 'member-1',
          memberMetadata: MemberMetadata.encode({
            version: assigner.version,
            topics: ['topic-A', 'topic-B'],
          }),
        },
        {
          memberId: 'member-2',
          memberMetadata: MemberMetadata.encode({
            version: assigner.version,
            topics: ['topic-B', 'topic-C'],
          }),
        },
      ]

      const assignment = await assigner.assign({
        members,
        topics: ['topic-A', 'topic-B', 'topic-C'],
      })

      expect(assignment).toEqual([
        {
          memberId: 'member-1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [0, 1, 2, 3],
              'topic-B': [0, 2],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-B': [1],
              'topic-C': [0, 1],
            },
          }),
        },
      ])
    })

    test('falls back safely when member metadata decode fails', async () => {
      metadata['topic-A'] = Array(2)
        .fill()
        .map((_, i) => ({ partitionId: i }))
      metadata['topic-B'] = Array(2)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [
        { memberId: 'member-1', memberMetadata: Buffer.from('invalid-metadata') },
        {
          memberId: 'member-2',
          memberMetadata: MemberMetadata.encode({
            version: assigner.version,
            topics: ['topic-B'],
          }),
        },
      ]

      const assignment = await assigner.assign({
        members,
        topics: ['topic-A', 'topic-B'],
      })

      expect(assignment).toEqual([
        {
          memberId: 'member-1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [0, 1],
              'topic-B': [0],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-B': [1],
            },
          }),
        },
      ])
    })

    test('rolling deployment regression: v1[A] + v2[A,B] keeps B assignable', async () => {
      metadata['topic-A'] = Array(2)
        .fill()
        .map((_, i) => ({ partitionId: i }))
      metadata['topic-B'] = Array(3)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [
        {
          memberId: 'old-v1',
          memberMetadata: MemberMetadata.encode({
            version: assigner.version,
            topics: ['topic-A'],
          }),
        },
        {
          memberId: 'new-v2',
          memberMetadata: MemberMetadata.encode({
            version: assigner.version,
            topics: ['topic-A', 'topic-B'],
          }),
        },
      ]

      const assignment = await assigner.assign({
        members,
        topics: ['topic-A', 'topic-B'],
      })

      expect(assignment).toEqual([
        {
          memberId: 'new-v2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [0],
              'topic-B': [0, 1, 2],
            },
          }),
        },
        {
          memberId: 'old-v1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [1],
            },
          }),
        },
      ])

      const byMember = assignment.reduce((acc, { memberId, memberAssignment }) => {
        acc[memberId] = MemberAssignment.decode(memberAssignment).assignment
        return acc
      }, {})

      expect(new Set(byMember['new-v2']['topic-B'])).toEqual(new Set([0, 1, 2]))
      expect(byMember['old-v1']['topic-B']).toBeUndefined()

      const seen = new Set()
      for (const { memberAssignment } of assignment) {
        const decoded = MemberAssignment.decode(memberAssignment).assignment
        for (const [topic, partitions] of Object.entries(decoded)) {
          for (const partition of partitions) {
            const key = `${topic}:${partition}`
            expect(seen.has(key)).toBe(false)
            seen.add(key)
          }
        }
      }
      expect(seen).toEqual(
        new Set(['topic-A:0', 'topic-A:1', 'topic-B:0', 'topic-B:1', 'topic-B:2'])
      )
    })

    test('documents topic-scoped round-robin contract', async () => {
      metadata['topic-A'] = Array(4)
        .fill()
        .map((_, i) => ({ partitionId: i }))
      metadata['topic-B'] = Array(4)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [{ memberId: 'member-1' }, { memberId: 'member-2' }]

      const assignment = await assigner.assign({ members, topics: ['topic-A', 'topic-B'] })

      expect(assignment).toEqual([
        {
          memberId: 'member-1',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [0, 2],
              'topic-B': [0, 2],
            },
          }),
        },
        {
          memberId: 'member-2',
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: {
              'topic-A': [1, 3],
              'topic-B': [1, 3],
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
