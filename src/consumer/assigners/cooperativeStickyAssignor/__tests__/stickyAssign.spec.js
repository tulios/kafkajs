const { stickyAssign } = require('../stickyAssign')
const { partitionKey } = require('../partitionKey')

const member = (memberId, topics, ownedPartitions = {}, generationId = -1) => ({
  memberId,
  topics,
  ownedPartitions,
  generationId,
})

const counts = assignment => Object.keys(assignment).map(id => assignment[id].length)

const allAssignedKeys = assignment => {
  const keys = []
  for (const id of Object.keys(assignment)) {
    for (const tp of assignment[id]) keys.push(partitionKey(tp))
  }
  return keys
}

describe('Consumer > assigners > cooperativeSticky > stickyAssign', () => {
  test('from-scratch assignment is balanced (≤1 skew)', () => {
    const assignment = stickyAssign({
      members: [member('m1', ['t']), member('m2', ['t']), member('m3', ['t'])],
      topicPartitions: { t: [0, 1, 2, 3, 4, 5, 6] },
    })

    const c = counts(assignment)
    expect(Math.max(...c) - Math.min(...c)).toBeLessThanOrEqual(1)
    expect(allAssignedKeys(assignment).sort()).toEqual(
      [0, 1, 2, 3, 4, 5, 6].map(p => partitionKey({ topic: 't', partition: p })).sort()
    )
  })

  test('re-running with current assignment as owned → zero movement', () => {
    const first = stickyAssign({
      members: [member('m1', ['t']), member('m2', ['t']), member('m3', ['t'])],
      topicPartitions: { t: [0, 1, 2, 3, 4, 5] },
    })

    const owned = {}
    for (const id of Object.keys(first)) {
      owned[id] = {}
      for (const tp of first[id]) {
        if (!owned[id][tp.topic]) owned[id][tp.topic] = []
        owned[id][tp.topic].push(tp.partition)
      }
    }

    const second = stickyAssign({
      members: [
        member('m1', ['t'], owned.m1, 5),
        member('m2', ['t'], owned.m2, 5),
        member('m3', ['t'], owned.m3, 5),
      ],
      topicPartitions: { t: [0, 1, 2, 3, 4, 5] },
    })

    for (const id of Object.keys(first)) {
      expect(allKeysOf(second[id]).sort()).toEqual(allKeysOf(first[id]).sort())
    }
  })

  test('adding a member moves only the minimum partitions needed', () => {
    // m1+m2 own 3 each; add m3. Balanced target is 2/2/2 → m3 takes 2, one each from m1,m2.
    const assignment = stickyAssign({
      members: [
        member('m1', ['t'], { t: [0, 1, 2] }, 5),
        member('m2', ['t'], { t: [3, 4, 5] }, 5),
        member('m3', ['t'], {}, -1),
      ],
      topicPartitions: { t: [0, 1, 2, 3, 4, 5] },
    })

    const c = counts(assignment)
    expect(Math.max(...c) - Math.min(...c)).toBeLessThanOrEqual(1)
    expect(assignment.m3.length).toBe(2)
    // m1 and m2 each retain 2 of their original 3.
    expect(assignment.m1.length).toBe(2)
    expect(assignment.m2.length).toBe(2)
    expect(assignment.m1.every(tp => [0, 1, 2].includes(tp.partition))).toBe(true)
    expect(assignment.m2.every(tp => [3, 4, 5].includes(tp.partition))).toBe(true)
  })

  test('heterogeneous subscriptions are balanced per topic', () => {
    const assignment = stickyAssign({
      members: [member('m1', ['a', 'b']), member('m2', ['a']), member('m3', ['b'])],
      topicPartitions: { a: [0, 1, 2, 3], b: [0, 1, 2, 3] },
    })

    // Every partition assigned once, only to subscribers.
    expect(allAssignedKeys(assignment).length).toBe(8)
    for (const tp of assignment.m2) expect(tp.topic).toBe('a')
    for (const tp of assignment.m3) expect(tp.topic).toBe('b')
  })

  test('never assigns a partition to a non-subscriber', () => {
    const assignment = stickyAssign({
      members: [member('m1', ['a']), member('m2', ['b'])],
      topicPartitions: { a: [0, 1], b: [0, 1] },
    })

    for (const tp of assignment.m1) expect(tp.topic).toBe('a')
    for (const tp of assignment.m2) expect(tp.topic).toBe('b')
  })

  test('generation conflict: higher generation keeps the partition', () => {
    const assignment = stickyAssign({
      members: [
        member('m1', ['t'], { t: [0] }, 5),
        member('m2', ['t'], { t: [0] }, 7), // higher gen wins partition 0
      ],
      topicPartitions: { t: [0, 1] },
    })

    expect(assignment.m2.some(tp => tp.partition === 0)).toBe(true)
    expect(assignment.m1.some(tp => tp.partition === 0)).toBe(false)
  })

  test('generation conflict: equal generation frees the partition (reassigned once)', () => {
    const assignment = stickyAssign({
      members: [
        member('m1', ['t'], { t: [0] }, 5),
        member('m2', ['t'], { t: [0] }, 5), // equal gen → freed, then reassigned to balance
      ],
      topicPartitions: { t: [0, 1] },
    })

    // Partition 0 still assigned exactly once across the group.
    const owners = ['m1', 'm2'].filter(id => assignment[id].some(tp => tp.partition === 0))
    expect(owners.length).toBe(1)
    expect(allAssignedKeys(assignment).length).toBe(2)
  })

  test('drops owned partitions that no longer exist in metadata (S4)', () => {
    const assignment = stickyAssign({
      members: [member('m1', ['t'], { t: [0, 1, 2, 3, 4] }, 5)],
      topicPartitions: { t: [0, 1, 2] }, // topic shrunk to 3 partitions
    })

    expect(assignment.m1.map(tp => tp.partition).sort()).toEqual([0, 1, 2])
  })

  test('single-member group gets everything', () => {
    const assignment = stickyAssign({
      members: [member('m1', ['t'])],
      topicPartitions: { t: [0, 1, 2, 3] },
    })
    expect(assignment.m1.length).toBe(4)
  })

  // Property-based: random topologies always satisfy invariants.
  test('property: random topologies are balanced and assigned exactly once', () => {
    const rand = seededRandom(12345)
    for (let trial = 0; trial < 200; trial++) {
      const numMembers = 1 + Math.floor(rand() * 5)
      const numPartitions = 1 + Math.floor(rand() * 20)
      const members = []
      for (let i = 0; i < numMembers; i++) members.push(member(`m${i}`, ['t']))
      const partitions = Array.from({ length: numPartitions }, (_, i) => i)

      const assignment = stickyAssign({ members, topicPartitions: { t: partitions } })

      // Exactly once.
      expect(allAssignedKeys(assignment).sort()).toEqual(
        partitions.map(p => partitionKey({ topic: 't', partition: p })).sort()
      )
      // Balanced (single subscription group → global ≤1 skew).
      const c = counts(assignment)
      expect(Math.max(...c) - Math.min(...c)).toBeLessThanOrEqual(1)
    }
  })

  test('property: re-run after assignment never moves a partition when balanced', () => {
    const rand = seededRandom(999)
    for (let trial = 0; trial < 100; trial++) {
      const numMembers = 1 + Math.floor(rand() * 4)
      const numPartitions = numMembers + Math.floor(rand() * 12)
      const members = []
      for (let i = 0; i < numMembers; i++) members.push(member(`m${i}`, ['t']))
      const partitions = Array.from({ length: numPartitions }, (_, i) => i)

      const first = stickyAssign({ members, topicPartitions: { t: partitions } })

      const ownedMembers = members.map(m => {
        const owned = {}
        for (const tp of first[m.memberId]) {
          if (!owned[tp.topic]) owned[tp.topic] = []
          owned[tp.topic].push(tp.partition)
        }
        return member(m.memberId, ['t'], owned, 5)
      })

      const second = stickyAssign({ members: ownedMembers, topicPartitions: { t: partitions } })

      for (const m of members) {
        expect(allKeysOf(second[m.memberId]).sort()).toEqual(allKeysOf(first[m.memberId]).sort())
      }
    }
  })
})

const allKeysOf = partitions => partitions.map(partitionKey)

// Deterministic PRNG (mulberry32) so property tests are reproducible.
function seededRandom(seed) {
  let a = seed
  return function() {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
