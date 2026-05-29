const { computeTransferring, adjust } = require('../cooperative')
const { stickyAssign } = require('../stickyAssign')
const { partitionKey } = require('../partitionKey')
const createAssigner = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../../assignerProtocol')
const { UserData } = require('../userData')

const sub = (memberId, ownedPartitions) => ({ memberId, ownedPartitions })

describe('Consumer > assigners > cooperativeSticky > cooperative', () => {
  describe('computeTransferring + adjust', () => {
    test('detects a partition transferring from old to new owner', () => {
      const subscriptions = [sub('m1', { t: [0, 1] }), sub('m2', { t: [] })]
      // ideal wants to move partition 1 to m2, but m1 still owns it
      const ideal = {
        m1: [{ topic: 't', partition: 0 }],
        m2: [{ topic: 't', partition: 1 }],
      }

      const transferring = computeTransferring(subscriptions, ideal)
      expect(transferring.has(partitionKey({ topic: 't', partition: 1 }))).toBe(true)
      expect(transferring.size).toBe(1)
    })

    test('transferring partition is stripped from BOTH owners in phase 1', () => {
      const subscriptions = [sub('m1', { t: [0, 1] }), sub('m2', { t: [] })]
      const ideal = {
        m1: [{ topic: 't', partition: 0 }],
        m2: [{ topic: 't', partition: 1 }],
      }
      const transferring = computeTransferring(subscriptions, ideal)
      const adjusted = adjust(ideal, transferring)

      expect(adjusted.m1).toEqual([{ topic: 't', partition: 0 }])
      expect(adjusted.m2).toEqual([]) // m2 does NOT get partition 1 yet
    })

    test('phase 2: once old owner releases it, freed partition IS assigned', () => {
      // m1 has released partition 1 (no longer owned); m2 still wants it
      const subscriptions = [sub('m1', { t: [0] }), sub('m2', { t: [] })]
      const ideal = {
        m1: [{ topic: 't', partition: 0 }],
        m2: [{ topic: 't', partition: 1 }],
      }
      const transferring = computeTransferring(subscriptions, ideal)
      expect(transferring.size).toBe(0) // nobody else owns partition 1

      const adjusted = adjust(ideal, transferring)
      expect(adjusted.m2).toEqual([{ topic: 't', partition: 1 }])
    })

    test('steady-state re-run: nothing transferring, no-op', () => {
      const subscriptions = [sub('m1', { t: [0] }), sub('m2', { t: [1] })]
      const ideal = {
        m1: [{ topic: 't', partition: 0 }],
        m2: [{ topic: 't', partition: 1 }],
      }
      const transferring = computeTransferring(subscriptions, ideal)
      expect(transferring.size).toBe(0)
      expect(adjust(ideal, transferring)).toEqual(ideal)
    })

    test('converges in two rounds: add a member, scale-down owner releases', () => {
      const topicPartitions = { t: [0, 1, 2, 3] }

      // Round 1: m1 owns all 4, m2 joins. ideal = 2/2, partitions 2,3 transfer.
      const r1Subs = [
        { memberId: 'm1', topics: ['t'], ownedPartitions: { t: [0, 1, 2, 3] }, generationId: 5 },
        { memberId: 'm2', topics: ['t'], ownedPartitions: {}, generationId: -1 },
      ]
      const ideal1 = stickyAssign({ members: r1Subs, topicPartitions })
      const transferring1 = computeTransferring(r1Subs, ideal1)
      const adjusted1 = adjust(ideal1, transferring1)

      // m2 gets nothing yet; m1 keeps only its non-transferring partitions.
      expect(adjusted1.m2.length).toBe(0)
      expect(transferring1.size).toBeGreaterThan(0)

      // Round 2: m1 advertises the reduced owned set (what it kept).
      const m1Kept = adjusted1.m1.map(tp => tp.partition)
      const r2Subs = [
        { memberId: 'm1', topics: ['t'], ownedPartitions: { t: m1Kept }, generationId: 6 },
        { memberId: 'm2', topics: ['t'], ownedPartitions: {}, generationId: -1 },
      ]
      const ideal2 = stickyAssign({ members: r2Subs, topicPartitions })
      const transferring2 = computeTransferring(r2Subs, ideal2)
      const adjusted2 = adjust(ideal2, transferring2)

      // Converged: balanced 2/2, nothing transferring.
      expect(transferring2.size).toBe(0)
      expect(adjusted2.m1.length).toBe(2)
      expect(adjusted2.m2.length).toBe(2)
    })
  })

  describe('assigner contract', () => {
    const cluster = {
      findTopicPartitionMetadata: topic => {
        const counts = { 'topic-A': 4, 'topic-B': 2 }
        return Array.from({ length: counts[topic] || 0 }, (_, i) => ({ partitionId: i }))
      },
    }

    const buildMember = (memberId, topics, ownedPartitions = {}, generationId = -1) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({
        version: 0,
        topics,
        userData: UserData.encode({ generationId, ownedPartitions }),
      }),
    })

    test('assign output decodes to valid MemberAssignment buffers', async () => {
      const assigner = createAssigner({ cluster })
      const result = await assigner.assign({
        members: [buildMember('m1', ['topic-A']), buildMember('m2', ['topic-A'])],
        topics: ['topic-A'],
      })

      expect(result.length).toBe(2)
      for (const { memberId, memberAssignment } of result) {
        expect(typeof memberId).toBe('string')
        const decoded = MemberAssignment.decode(memberAssignment)
        expect(decoded).not.toBeNull()
        expect(decoded.version).toBe(0)
      }
    })

    test('from-scratch assign is balanced across the group', async () => {
      const assigner = createAssigner({ cluster })
      const result = await assigner.assign({
        members: [buildMember('m1', ['topic-A']), buildMember('m2', ['topic-A'])],
        topics: ['topic-A'],
      })

      const sizes = result.map(({ memberAssignment }) => {
        const { assignment } = MemberAssignment.decode(memberAssignment)
        return Object.values(assignment).reduce((sum, ps) => sum + ps.length, 0)
      })
      expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1)
    })

    test('single-member group gets everything', async () => {
      const assigner = createAssigner({ cluster })
      const result = await assigner.assign({
        members: [buildMember('m1', ['topic-A', 'topic-B'])],
        topics: ['topic-A', 'topic-B'],
      })

      const { assignment } = MemberAssignment.decode(result[0].memberAssignment)
      expect(assignment['topic-A'].length).toBe(4)
      expect(assignment['topic-B'].length).toBe(2)
    })

    test('protocol() encodes topics + ownedPartitions into metadata', () => {
      const assigner = createAssigner({ cluster })
      const { name, metadata } = assigner.protocol({
        topics: ['topic-A'],
        ownedPartitions: { 'topic-A': [0, 1] },
        generationId: 9,
      })

      expect(name).toBe('cooperative-sticky')
      const decoded = MemberMetadata.decode(metadata)
      expect(decoded.topics).toEqual(['topic-A'])
      const ud = UserData.decode(decoded.userData)
      expect(ud.generationId).toBe(9)
      expect(ud.ownedPartitions).toEqual({ 'topic-A': [0, 1] })
    })

    test('protocol() works with defaults (standalone)', () => {
      const assigner = createAssigner({ cluster })
      const { metadata } = assigner.protocol({ topics: ['topic-A'] })
      const ud = UserData.decode(MemberMetadata.decode(metadata).userData)
      expect(ud.generationId).toBe(-1)
      expect(ud.ownedPartitions).toEqual({})
    })

    test('exposes cooperative rebalanceProtocol flag', () => {
      const assigner = createAssigner({ cluster })
      expect(assigner.rebalanceProtocol).toBe('cooperative')
    })

    test('phase-1 assign withholds a transferring partition', async () => {
      const assigner = createAssigner({ cluster })
      // m1 owns all 4 of topic-A; m2 joins empty → some partitions must transfer
      const result = await assigner.assign({
        members: [
          buildMember('m1', ['topic-A'], { 'topic-A': [0, 1, 2, 3] }, 5),
          buildMember('m2', ['topic-A'], {}, -1),
        ],
        topics: ['topic-A'],
      })

      const byId = Object.fromEntries(
        result.map(({ memberId, memberAssignment }) => [
          memberId,
          MemberAssignment.decode(memberAssignment).assignment,
        ])
      )

      const m2Count = Object.values(byId.m2 || {}).reduce((s, ps) => s + ps.length, 0)
      // m2 receives nothing in phase 1 (its partitions are still owned by m1)
      expect(m2Count).toBe(0)
    })
  })
})
