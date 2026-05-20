const CooperativeStickyAssigner = require('./index')
const { encodeUserData, decodeUserData, decodeAssignmentUserData } = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

const makeCluster = topicCounts => {
  const metadata = {}
  for (const [topic, count] of Object.entries(topicCounts)) {
    metadata[topic] = Array.from({ length: count }, (_, i) => ({ partitionId: i }))
  }
  return { findTopicPartitionMetadata: topic => metadata[topic] || [] }
}

const makeMember = (memberId, topics, ownedPartitions = {}, generationId = -1) => ({
  memberId,
  memberMetadata: MemberMetadata.encode({
    version: 1,
    topics,
    userData: encodeUserData(generationId, ownedPartitions),
  }),
})

const decodeAssignment = buffer => MemberAssignment.decode(buffer).assignment

const assignAndDecode = async (assigner, members, topics) => {
  const result = await assigner.assign({ members, topics })
  return result.reduce((acc, { memberId, memberAssignment }) => {
    acc[memberId] = decodeAssignment(memberAssignment)
    return acc
  }, {})
}

describe('Consumer > assigners > CooperativeStickyAssigner', () => {
  test('protocol name is cooperative-sticky-kafkajs and metadata includes userData', () => {
    const assigner = CooperativeStickyAssigner({ cluster: makeCluster({ A: 1 }) })
    const { name, metadata } = assigner.protocol({ topics: ['A'] })

    expect(name).toBe('cooperative-sticky-kafkajs')
    const decoded = MemberMetadata.decode(metadata)
    expect(decoded.topics).toEqual(['A'])
    expect(decodeUserData(decoded.userData)).toEqual({ generationId: -1, ownedPartitions: {} })
  })

  test('decodeUserData is resilient to malformed bytes', () => {
    expect(decodeUserData(Buffer.from('bad'))).toEqual({ generationId: -1, ownedPartitions: {} })
  })

  test('assign is resilient to malformed member metadata', async () => {
    const assigner = CooperativeStickyAssigner({ cluster: makeCluster({ A: 2 }) })
    const members = [
      { memberId: 'm1', memberMetadata: Buffer.from('invalid') },
      makeMember('m2', ['A']),
    ]

    const assignments = await assignAndDecode(assigner, members, ['A'])
    expect(assignments.m1).toEqual({})
    expect((assignments.m2.A || []).sort()).toEqual([0, 1])
  })

  test('withholds transferred partitions in first round and completes in second round', async () => {
    const cluster = makeCluster({ A: 4 })
    const assigner = CooperativeStickyAssigner({ cluster })

    // Round 1: c1 owns all, c2 joins. Transfers should be withheld from c2.
    const round1 = await assignAndDecode(
      assigner,
      [makeMember('c1', ['A'], { A: [0, 1, 2, 3] }, 1), makeMember('c2', ['A'], {}, -1)],
      ['A']
    )

    expect((round1.c1.A || []).sort()).toEqual([0, 1])
    expect(round1.c2.A || []).toEqual([])

    // c1 persists round-0 ownership, then revokes in round-1
    expect(assigner.onAssignment({ assignment: { A: [0, 1, 2, 3] }, generationId: 1 })).toBe(false)
    expect(assigner.onAssignment({ assignment: { A: [0, 1] }, generationId: 2 })).toBe(true)
    const c1MetadataR2 = assigner.protocol({ topics: ['A'] }).metadata

    // Round 2: previously transferred partitions are now unowned and can be assigned
    const round2 = await assignAndDecode(
      assigner,
      [{ memberId: 'c1', memberMetadata: c1MetadataR2 }, makeMember('c2', ['A'], {}, 2)],
      ['A']
    )

    expect((round2.c1.A || []).sort()).toEqual([0, 1])
    expect((round2.c2.A || []).sort()).toEqual([2, 3])
  })

  test('marks all members for rejoin when cooperative transfer is withheld', async () => {
    const cluster = makeCluster({ A: 4 })
    const assigner = CooperativeStickyAssigner({ cluster })

    const result = await assigner.assign({
      members: [makeMember('c1', ['A'], { A: [0, 1, 2, 3] }, 1), makeMember('c2', ['A'], {}, -1)],
      topics: ['A'],
      allSubscribedTopics: ['A'],
    })

    for (const { memberAssignment } of result) {
      const decoded = MemberAssignment.decode(memberAssignment)
      expect(decodeAssignmentUserData(decoded.userData)).toEqual({ needsRejoin: true })
    }
  })

  test('rolling deployment scenario with mixed subscriptions keeps full eligible coverage', async () => {
    const assigner = CooperativeStickyAssigner({ cluster: makeCluster({ A: 2, B: 2 }) })
    const assignment = await assignAndDecode(
      assigner,
      [makeMember('old', ['A'], { A: [0, 1] }, 3), makeMember('new', ['A', 'B'], {}, -1)],
      ['A', 'B']
    )

    expect((assignment.old.A || []).sort()).toEqual([0, 1])
    expect(assignment.old.B || []).toEqual([])
    expect(assignment.new.A || []).toEqual([])
    expect((assignment.new.B || []).sort()).toEqual([0, 1])

    const seen = new Set()
    for (const [memberId, byTopic] of Object.entries(assignment)) {
      for (const [topic, partitions] of Object.entries(byTopic)) {
        for (const partition of partitions) {
          const key = `${topic}:${partition}`
          expect(seen.has(key)).toBe(false)
          seen.add(key)

          if (memberId === 'old') {
            expect(topic).toBe('A')
          }
        }
      }
    }
    expect(seen).toEqual(new Set(['A:0', 'A:1', 'B:0', 'B:1']))
  })
})
