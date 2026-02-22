const CooperativeStickyAssigner = require('./index')
const { encodeUserData, decodeUserData } = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Build a mock cluster whose findTopicPartitionMetadata returns the provided map.
 * @param {Object<string, number>} topicPartitionCounts  e.g. { 'topic-A': 3 }
 */
function makeCluster(topicPartitionCounts) {
  const metadata = {}
  for (const [topic, count] of Object.entries(topicPartitionCounts)) {
    metadata[topic] = Array.from({ length: count }, (_, i) => ({ partitionId: i }))
  }
  return { findTopicPartitionMetadata: topic => metadata[topic] || [] }
}

/**
 * Build a GroupMember whose metadata encodes the given owned partitions and generationId.
 * @param {string}                  memberId
 * @param {string[]}                subscribedTopics
 * @param {Object<string,number[]>} [ownedPartitions]
 * @param {number}                  [generationId]
 */
function makeMember(memberId, subscribedTopics, ownedPartitions = {}, generationId = -1) {
  return {
    memberId,
    memberMetadata: MemberMetadata.encode({
      version: 1,
      topics: subscribedTopics,
      userData: encodeUserData(generationId, ownedPartitions),
    }),
  }
}

/**
 * Decode a GroupMemberAssignment buffer into a plain { topic: [partitionId] } object.
 */
function decodeAssignment(memberAssignmentBuffer) {
  const decoded = MemberAssignment.decode(memberAssignmentBuffer)
  return decoded ? decoded.assignment : {}
}

/**
 * Run assign() and return a map  memberId → { topic: [partitionIds] }.
 */
async function runAssign(assigner, members, topics) {
  const result = await assigner.assign({ members, topics })
  return result.reduce((acc, { memberId, memberAssignment }) => {
    acc[memberId] = decodeAssignment(memberAssignment)
    return acc
  }, {})
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('Consumer > assigners > CooperativeStickyAssigner', () => {
  let cluster, assigner

  beforeEach(() => {
    cluster = makeCluster({ 'topic-A': 3, 'topic-B': 2 })
    assigner = CooperativeStickyAssigner({ cluster })
  })

  // ── userData codec ─────────────────────────────────────────────────────────

  describe('userData encode / decode', () => {
    test('round-trips generationId and owned partitions', () => {
      const ownedPartitions = { 'topic-A': [0, 2], 'topic-B': [1] }
      const buf = encodeUserData(5, ownedPartitions)
      const decoded = decodeUserData(buf)
      expect(decoded.generationId).toBe(5)
      expect(decoded.ownedPartitions).toEqual(ownedPartitions)
    })

    test('returns defaults for empty / null buffer', () => {
      expect(decodeUserData(null)).toEqual({
        generationId: -1,
        ownedPartitions: {},
      })
      expect(decodeUserData(Buffer.alloc(0))).toEqual({
        generationId: -1,
        ownedPartitions: {},
      })
    })

    test('handles DEFAULT_GENERATION (-1) and empty owned partitions', () => {
      const buf = encodeUserData(-1, {})
      const decoded = decodeUserData(buf)
      expect(decoded.generationId).toBe(-1)
      expect(decoded.ownedPartitions).toEqual({})
    })
  })

  // ── #protocol ──────────────────────────────────────────────────────────────

  describe('#protocol', () => {
    test('returns assigner name and encoded metadata', () => {
      const result = assigner.protocol({ topics: ['topic-A'] })
      expect(result.name).toBe('CooperativeStickyAssigner')
      const decoded = MemberMetadata.decode(result.metadata)
      expect(decoded.topics).toEqual(['topic-A'])
    })

    test('encodes DEFAULT_GENERATION when no assignment received yet', () => {
      const result = assigner.protocol({ topics: ['topic-A'] })
      const decoded = MemberMetadata.decode(result.metadata)
      const userData = decodeUserData(decoded.userData)
      expect(userData.generationId).toBe(-1)
      expect(userData.ownedPartitions).toEqual({})
    })

    test('encodes generationId and ownedPartitions after onAssignment', () => {
      assigner.onAssignment({ assignment: { 'topic-A': [0, 1] }, generationId: 3 })
      const result = assigner.protocol({ topics: ['topic-A'] })
      const decoded = MemberMetadata.decode(result.metadata)
      const userData = decodeUserData(decoded.userData)
      expect(userData.generationId).toBe(3)
      expect(userData.ownedPartitions).toEqual({ 'topic-A': [0, 1] })
    })
  })

  // ── #onAssignment ────────────────────────────────────────────────────────

  describe('#onAssignment', () => {
    test('returns false on the very first assignment (no prior state)', () => {
      const result = assigner.onAssignment({
        assignment: { 'topic-A': [0, 1] },
        generationId: 1,
      })
      expect(result).toBe(false)
    })

    test('returns false when the assignment is unchanged', () => {
      assigner.onAssignment({ assignment: { 'topic-A': [0, 1] }, generationId: 1 })
      const result = assigner.onAssignment({
        assignment: { 'topic-A': [0, 1] },
        generationId: 2,
      })
      expect(result).toBe(false)
    })

    test('returns false when partitions are only added', () => {
      assigner.onAssignment({ assignment: { 'topic-A': [0] }, generationId: 1 })
      const result = assigner.onAssignment({
        assignment: { 'topic-A': [0, 1] },
        generationId: 2,
      })
      expect(result).toBe(false)
    })

    test('returns true when a partition is revoked', () => {
      assigner.onAssignment({ assignment: { 'topic-A': [0, 1, 2] }, generationId: 1 })
      const result = assigner.onAssignment({
        assignment: { 'topic-A': [0, 1] },
        generationId: 2,
      })
      expect(result).toBe(true)
    })

    test('returns true when an entire topic is removed', () => {
      assigner.onAssignment({
        assignment: { 'topic-A': [0], 'topic-B': [0] },
        generationId: 1,
      })
      const result = assigner.onAssignment({
        assignment: { 'topic-A': [0] },
        generationId: 2,
      })
      expect(result).toBe(true)
    })
  })

  // ── #assign - fresh start (no owned partitions) ────────────────────────────

  describe('#assign - fresh start', () => {
    test('distributes all partitions evenly across members', async () => {
      // topic-A: 3 partitions, topic-B: 2 partitions → 5 total, 2 members
      // each member should get 2 or 3 partitions
      const members = [
        makeMember('member-1', ['topic-A', 'topic-B']),
        makeMember('member-2', ['topic-A', 'topic-B']),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A', 'topic-B'])

      const m1Total =
        (assignment['member-1']['topic-A'] || []).length +
        (assignment['member-1']['topic-B'] || []).length
      const m2Total =
        (assignment['member-2']['topic-A'] || []).length +
        (assignment['member-2']['topic-B'] || []).length

      expect(m1Total + m2Total).toBe(5)
      expect(Math.abs(m1Total - m2Total)).toBeLessThanOrEqual(1)
    })

    test('assigns all partitions when there is only one member', async () => {
      const members = [makeMember('member-1', ['topic-A', 'topic-B'])]
      const assignment = await runAssign(assigner, members, ['topic-A', 'topic-B'])

      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1, 2])
      expect((assignment['member-1']['topic-B'] || []).sort()).toEqual([0, 1])
    })

    test('handles a topic with a single partition and multiple members', async () => {
      cluster = makeCluster({ 'topic-A': 1 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A']),
        makeMember('member-2', ['topic-A']),
        makeMember('member-3', ['topic-A']),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      const allPartitions = [
        ...(assignment['member-1']['topic-A'] || []),
        ...(assignment['member-2']['topic-A'] || []),
        ...(assignment['member-3']['topic-A'] || []),
      ]
      expect(allPartitions).toEqual([0])
    })

    test('returns an entry for every member even if some get no partitions', async () => {
      cluster = makeCluster({ 'topic-A': 1 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [makeMember('member-1', ['topic-A']), makeMember('member-2', ['topic-A'])]
      const result = await assigner.assign({ members, topics: ['topic-A'] })
      const memberIds = result.map(r => r.memberId).sort()
      expect(memberIds).toEqual(['member-1', 'member-2'])
    })
  })

  // ── #assign - stickiness ───────────────────────────────────────────────────

  describe('#assign - stickiness', () => {
    test('preserves existing assignments when no rebalance is needed', async () => {
      // 4 partitions, 2 members - each previously owned 2.  Nothing should move.
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 1),
        makeMember('member-2', ['topic-A'], { 'topic-A': [2, 3] }, 1),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1])
      expect((assignment['member-2']['topic-A'] || []).sort()).toEqual([2, 3])
    })

    test('gives new partitions to the under-assigned member while keeping existing ones', async () => {
      // Add a new partition (3) to a topic.  member-1 and member-2 already own [0,1] / [2].
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 1),
        makeMember('member-2', ['topic-A'], { 'topic-A': [2] }, 1),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // member-1 should still have [0, 1]; member-2 gets [2, 3]
      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1])
      expect((assignment['member-2']['topic-A'] || []).sort()).toEqual([2, 3])
    })

    test('keeps assignments sorted stably when all members rejoin cleanly', async () => {
      cluster = makeCluster({ 'topic-A': 6 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1, 2] }, 2),
        makeMember('member-2', ['topic-A'], { 'topic-A': [3, 4, 5] }, 2),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1, 2])
      expect((assignment['member-2']['topic-A'] || []).sort()).toEqual([3, 4, 5])
    })
  })

  // ── #assign - cooperative transfer ────────────────────────────────────────

  describe('#assign - cooperative transfer', () => {
    test('withholds a transferred partition from the new owner in the first rebalance', async () => {
      // Initial steady state: member-1 owns [0,1,2], member-2 owns [3]
      // Expected ideal: member-1→[0,1], member-2→[2,3]  (P2 transfers)
      // Cooperative rule: P2 must NOT appear in member-2's assignment this round.
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1, 2] }, 1),
        makeMember('member-2', ['topic-A'], { 'topic-A': [3] }, 1),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // Cooperative: P2 must NOT appear in member-2's assignment this round
      const m2Partitions = (assignment['member-2']['topic-A'] || []).sort()
      expect(m2Partitions).not.toContain(2)

      // member-1 gets its ideal (balanced) assignment [0,1]; it will revoke P2 after sync
      // which triggers a follow-up rebalance where member-2 can receive P2.
      const m1Partitions = (assignment['member-1']['topic-A'] || []).sort()
      expect(m1Partitions).toEqual([0, 1])
    })

    test('completes the transfer in the second rebalance after owner revokes', async () => {
      // Second rebalance: member-1 now only owns [0,1] (P2 was revoked), member-2 owns [3]
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 2),
        makeMember('member-2', ['topic-A'], { 'topic-A': [3] }, 2),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // Now P2 has no prior owner → no transfer → member-2 can receive it
      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1])
      expect((assignment['member-2']['topic-A'] || []).sort()).toEqual([2, 3])
    })

    test('a newly joining member does not trigger cooperative withholding for fresh partitions', async () => {
      // member-1 owned [0,1,2,3]; member-2 joins fresh.
      // Ideal: member-1→[0,1], member-2→[2,3].  P2 and P3 are transfers.
      // member-2 should get nothing in this round (cooperative).
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1, 2, 3] }, 1),
        makeMember('member-2', ['topic-A'], {}, -1), // fresh member, no owned partitions
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // Cooperative: P2 and P3 are transfers (member-1 loses them, member-2 would gain them),
      // so they are withheld from member-2 for this round.
      const m2Total = Object.values(assignment['member-2']).reduce((s, p) => s + p.length, 0)
      expect(m2Total).toBe(0)

      // member-1 gets its ideal balanced assignment [0,1]; it revokes P2,P3 after sync,
      // triggering a follow-up rebalance where member-2 finally receives them.
      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1])
    })
  })

  // ── #assign - quota balancing (new member joins a full group) ──────────────

  describe('#assign - quota balancing', () => {
    test('rebalances when 4th consumer joins 3 consumers with 6 partitions (2,2,2 → 2,2,1,1)', async () => {
      // This is the scenario: 6 partitions evenly split across 3 consumers,
      // then a 4th consumer joins.  minQuota=1, maxQuota=2, only 2 members may
      // keep maxQuota.  The 3rd must drop to minQuota so the 4th gets a partition.
      cluster = makeCluster({ 'topic-A': 6 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 3),
        makeMember('c2', ['topic-A'], { 'topic-A': [2, 3] }, 3),
        makeMember('c3', ['topic-A'], { 'topic-A': [4, 5] }, 3),
        makeMember('c4', ['topic-A'], {}, -1), // new member
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      const counts = ['c1', 'c2', 'c3', 'c4'].map(m => (assignment[m]['topic-A'] || []).length)
      const total = counts.reduce((a, b) => a + b, 0)

      // All 6 partitions must be accounted for (some may be withheld from c4
      // due to cooperative transfer, but no partition should be lost)
      // In round 1, c4 gets nothing (cooperative withholding), but the
      // balanced assignment must be (2, 2, 1, 0) — one member trimmed to 1.
      expect(total).toBe(5) // 6 minus 1 withheld transfer
      expect(counts.filter(c => c === 2)).toHaveLength(2)
      expect(counts.filter(c => c === 1)).toHaveLength(1)
      expect(counts.filter(c => c === 0)).toHaveLength(1) // c4: cooperative withholding
    })

    test('rebalances when 5th consumer joins 4 consumers with 6 partitions', async () => {
      // 6 partitions, 4 consumers (2,2,1,1), 5th joins → (2,1,1,1,1)
      // minQuota=1, maxQuota=2, only 1 member may keep maxQuota
      cluster = makeCluster({ 'topic-A': 6 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 4),
        makeMember('c2', ['topic-A'], { 'topic-A': [2, 3] }, 4),
        makeMember('c3', ['topic-A'], { 'topic-A': [4] }, 4),
        makeMember('c4', ['topic-A'], { 'topic-A': [5] }, 4),
        makeMember('c5', ['topic-A'], {}, -1), // new member
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      const counts = ['c1', 'c2', 'c3', 'c4', 'c5'].map(
        m => (assignment[m]['topic-A'] || []).length
      )

      // One member keeps maxQuota (2), one member drops from 2→1 (revokes 1),
      // that revoked partition is withheld from c5 (cooperative).
      // Round 1 result: (2, 1, 1, 1, 0)
      expect(counts.filter(c => c === 2)).toHaveLength(1)
      expect(counts.filter(c => c === 1)).toHaveLength(3)
      expect(counts.filter(c => c === 0)).toHaveLength(1)
    })

    test('6th consumer joining 5 consumers with 6 partitions gives everyone 1', async () => {
      // 6 partitions, 6 consumers → minQuota=1, maxQuota=1, everyone gets 1
      cluster = makeCluster({ 'topic-A': 6 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 5),
        makeMember('c2', ['topic-A'], { 'topic-A': [2] }, 5),
        makeMember('c3', ['topic-A'], { 'topic-A': [3] }, 5),
        makeMember('c4', ['topic-A'], { 'topic-A': [4] }, 5),
        makeMember('c5', ['topic-A'], { 'topic-A': [5] }, 5),
        makeMember('c6', ['topic-A'], {}, -1), // new member
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      const counts = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6'].map(
        m => (assignment[m]['topic-A'] || []).length
      )

      // c1 drops from 2→1 (revokes 1 partition), withheld from c6
      // Round 1: (1, 1, 1, 1, 1, 0)
      expect(counts.filter(c => c === 1)).toHaveLength(5)
      expect(counts.filter(c => c === 0)).toHaveLength(1) // c6: cooperative withholding
    })
  })

  // ── #assign - member leaving ───────────────────────────────────────────────

  describe('#assign - member leaves', () => {
    test('redistributes orphaned partitions to the remaining member', async () => {
      // member-2 crashes and does not rejoin.
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      // Only member-1 is present; it previously owned [0,1].
      // Partitions [2,3] (previously owned by the departed member-2) are unowned.
      const members = [makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 1)]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // member-1 must get all 4 partitions
      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 1, 2, 3])
    })

    test('balances load after a member leaves', async () => {
      cluster = makeCluster({ 'topic-A': 6 })
      assigner = CooperativeStickyAssigner({ cluster })

      // Three members each owned 2 partitions; now only two remain.
      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 2),
        makeMember('member-2', ['topic-A'], { 'topic-A': [2, 3] }, 2),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      const m1Count = (assignment['member-1']['topic-A'] || []).length
      const m2Count = (assignment['member-2']['topic-A'] || []).length

      expect(m1Count + m2Count).toBe(6)
      expect(Math.abs(m1Count - m2Count)).toBeLessThanOrEqual(1)
    })
  })

  // ── #assign - conflict resolution ─────────────────────────────────────────

  describe('#assign - conflict resolution', () => {
    test('treats a partition claimed by two members in the same generation as unowned', async () => {
      // Both member-1 and member-2 claim topic-A partition 0 in generation 2.
      cluster = makeCluster({ 'topic-A': 4 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0, 1] }, 2),
        makeMember('member-2', ['topic-A'], { 'topic-A': [0, 2] }, 2), // also claims P0
        makeMember('member-3', ['topic-A'], { 'topic-A': [3] }, 2),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // P0 is contested → treated as unowned this round.
      // It must appear in exactly one member's assignment.
      const allP0Owners = ['member-1', 'member-2', 'member-3'].filter(m =>
        (assignment[m]['topic-A'] || []).includes(0)
      )
      expect(allP0Owners).toHaveLength(1)

      // Total assigned partitions should be 4
      const total = ['member-1', 'member-2', 'member-3'].reduce(
        (s, m) => s + (assignment[m]['topic-A'] || []).length,
        0
      )
      expect(total).toBe(4)
    })

    test('higher-generation member wins a cross-generation conflict', async () => {
      // member-1 (gen 3) and member-2 (gen 2) both claim partition 0.
      // member-1 should win because gen 3 > gen 2.
      cluster = makeCluster({ 'topic-A': 2 })
      assigner = CooperativeStickyAssigner({ cluster })

      const members = [
        makeMember('member-1', ['topic-A'], { 'topic-A': [0] }, 3),
        makeMember('member-2', ['topic-A'], { 'topic-A': [0, 1] }, 2),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A'])

      // P0 should be with member-1 (higher generation)
      expect((assignment['member-1']['topic-A'] || []).includes(0)).toBe(true)
      expect((assignment['member-2']['topic-A'] || []).includes(0)).toBe(false)
    })
  })

  // ── #assign - multiple topics / mixed subscriptions ───────────────────────

  describe('#assign - multiple topics', () => {
    test('correctly assigns partitions across multiple topics', async () => {
      const members = [
        makeMember('member-1', ['topic-A', 'topic-B']),
        makeMember('member-2', ['topic-A', 'topic-B']),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A', 'topic-B'])

      const allAssigned = [
        ...(assignment['member-1']['topic-A'] || []),
        ...(assignment['member-1']['topic-B'] || []),
        ...(assignment['member-2']['topic-A'] || []),
        ...(assignment['member-2']['topic-B'] || []),
      ]
      // All 5 partitions (3 + 2) must be assigned exactly once
      expect(allAssigned.length).toBe(5)
    })

    test('preserves assignments across multiple topics on rejoin', async () => {
      const members = [
        makeMember('member-1', ['topic-A', 'topic-B'], { 'topic-A': [0, 2], 'topic-B': [0] }, 1),
        makeMember('member-2', ['topic-A', 'topic-B'], { 'topic-A': [1], 'topic-B': [1] }, 1),
      ]
      const assignment = await runAssign(assigner, members, ['topic-A', 'topic-B'])

      expect((assignment['member-1']['topic-A'] || []).sort()).toEqual([0, 2])
      expect((assignment['member-1']['topic-B'] || []).sort()).toEqual([0])
      expect((assignment['member-2']['topic-A'] || []).sort()).toEqual([1])
      expect((assignment['member-2']['topic-B'] || []).sort()).toEqual([1])
    })
  })

  // ── end-to-end lifecycle ───────────────────────────────────────────────────

  describe('end-to-end lifecycle', () => {
    test('two-round quota rebalance: 3→4 consumers with 6 partitions', async () => {
      cluster = makeCluster({ 'topic-A': 6 })

      // ── Steady state: 3 consumers each own 2 partitions ──────────────────
      const assigner1 = CooperativeStickyAssigner({ cluster })
      const steadyMembers = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 3),
        makeMember('c2', ['topic-A'], { 'topic-A': [2, 3] }, 3),
        makeMember('c3', ['topic-A'], { 'topic-A': [4, 5] }, 3),
      ]
      const steady = await runAssign(assigner1, steadyMembers, ['topic-A'])
      expect((steady['c1']['topic-A'] || []).sort()).toEqual([0, 1])
      expect((steady['c2']['topic-A'] || []).sort()).toEqual([2, 3])
      expect((steady['c3']['topic-A'] || []).sort()).toEqual([4, 5])

      // Simulate c3 receiving its assignment via onAssignment
      assigner1.onAssignment({ assignment: { 'topic-A': [4, 5] }, generationId: 3 })

      // ── Round 1: c4 joins — one consumer must drop to minQuota ───────────
      const c3Meta = assigner1.protocol({ topics: ['topic-A'] }).metadata
      const round1Members = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 3),
        makeMember('c2', ['topic-A'], { 'topic-A': [2, 3] }, 3),
        { memberId: 'c3', memberMetadata: c3Meta },
        makeMember('c4', ['topic-A'], {}, -1),
      ]
      const round1 = await runAssign(assigner1, round1Members, ['topic-A'])

      // c1 and c2 keep maxQuota(2), c3 drops to minQuota(1), c4 gets 0 (withheld)
      const r1Counts = ['c1', 'c2', 'c3', 'c4'].map(m => (round1[m]['topic-A'] || []).length)
      expect(r1Counts).toEqual([2, 2, 1, 0])

      // c3's revoked partition
      const c3Revoked = [4, 5].filter(p => !(round1['c3']['topic-A'] || []).includes(p))
      expect(c3Revoked).toHaveLength(1)

      // onAssignment for c3 should detect revocation
      const c3NewAssignment = round1['c3']['topic-A'] || []
      assigner1.onAssignment({ assignment: { 'topic-A': c3NewAssignment }, generationId: 4 })

      // ── Round 2: revoked partition is now unowned → c4 receives it ───────
      const c3MetaR2 = assigner1.protocol({ topics: ['topic-A'] }).metadata
      const round2Members = [
        makeMember('c1', ['topic-A'], { 'topic-A': [0, 1] }, 4),
        makeMember('c2', ['topic-A'], { 'topic-A': [2, 3] }, 4),
        { memberId: 'c3', memberMetadata: c3MetaR2 },
        makeMember('c4', ['topic-A'], {}, 4),
      ]
      const round2 = await runAssign(assigner1, round2Members, ['topic-A'])

      const r2Counts = ['c1', 'c2', 'c3', 'c4'].map(m => (round2[m]['topic-A'] || []).length)
      expect(r2Counts).toEqual([2, 2, 1, 1])

      // All 6 partitions assigned
      const allPartitions = ['c1', 'c2', 'c3', 'c4']
        .flatMap(m => round2[m]['topic-A'] || [])
        .sort((a, b) => a - b)
      expect(allPartitions).toEqual([0, 1, 2, 3, 4, 5])
    })

    test('two-round cooperative rebalance when a new member joins', async () => {
      cluster = makeCluster({ 'topic-A': 4 })

      // ── Round 1: single consumer owns all 4 partitions ───────────────────
      const assigner1 = CooperativeStickyAssigner({ cluster })
      const round1Members = [makeMember('c1', ['topic-A'])]
      const round1 = await runAssign(assigner1, round1Members, ['topic-A'])
      expect((round1['c1']['topic-A'] || []).sort()).toEqual([0, 1, 2, 3])

      // c1 receives the assignment and updates its state
      assigner1.onAssignment({ assignment: { 'topic-A': [0, 1, 2, 3] }, generationId: 1 })

      // ── Round 2: c2 joins.  c1 encodes its owned partitions. ─────────────
      // Decode c1's current protocol metadata to simulate what the broker sees
      const c1ProtocolMetadata = assigner1.protocol({ topics: ['topic-A'] }).metadata
      const c1Round2Member = { memberId: 'c1', memberMetadata: c1ProtocolMetadata }
      const c2Round2Member = makeMember('c2', ['topic-A']) // fresh, no owned partitions

      const round2 = await runAssign(assigner1, [c1Round2Member, c2Round2Member], ['topic-A'])

      // Cooperative: c2 should get nothing yet (all its would-be partitions transfer from c1)
      const c2Round2Total = Object.values(round2['c2']).reduce((s, p) => s + p.length, 0)
      expect(c2Round2Total).toBe(0)
      // c1 gets its balanced share [0,1]; partitions [2,3] are revoked from c1 (withheld from c2)
      expect((round2['c1']['topic-A'] || []).sort()).toEqual([0, 1])

      // c1's sync result is [0,1]; it revokes [2,3] so c2 can receive them in round 3
      // Simulate: c1's new assignment is [0,1], c2's is []
      assigner1.onAssignment({ assignment: { 'topic-A': [0, 1] }, generationId: 2 })

      // ── Round 3: c1 has revoked [2,3], c2 can now receive them ───────────
      const c1ProtocolMetadata3 = assigner1.protocol({ topics: ['topic-A'] }).metadata
      const c1Round3Member = { memberId: 'c1', memberMetadata: c1ProtocolMetadata3 }
      // c2 still owns nothing (it got [] in round 2)
      const c2Round3Member = makeMember('c2', ['topic-A'], {}, 2)

      const round3 = await runAssign(assigner1, [c1Round3Member, c2Round3Member], ['topic-A'])

      // Now P2 and P3 are unowned → no cooperative withholding → c2 gets them
      expect((round3['c1']['topic-A'] || []).sort()).toEqual([0, 1])
      expect((round3['c2']['topic-A'] || []).sort()).toEqual([2, 3])
    })
  })
})
