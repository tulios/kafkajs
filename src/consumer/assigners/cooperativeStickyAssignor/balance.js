const { partitionKey } = require('./partitionKey')

/**
 * Balance helpers for sticky assignment.
 *
 * A subscription "group" is a set of members that subscribe to exactly the same
 * topics. Within such a group, partition counts must differ by ≤1. With
 * heterogeneous subscriptions there is no single global quota, so we balance
 * greedily: free partitions go to the least-loaded subscribed member, then a
 * bounded rebalance pass moves partitions from over-loaded to under-loaded
 * members until no improving move remains.
 */

/**
 * Greedily assign each free partition to the subscribed member with the fewest
 * partitions so far. Deterministic: partitions and members are pre-sorted by
 * the caller; ties broken by memberId.
 *
 * @param {Array<{topic: string, partition: number}>} freePartitions
 * @param {Object<string, Array<{topic: string, partition: number}>>} assignment memberId → partitions (mutated)
 * @param {Object<string, string[]>} subscribersByTopic topic → subscribed memberIds
 */
const fillFreePartitions = (freePartitions, assignment, subscribersByTopic) => {
  for (const tp of freePartitions) {
    const candidates = subscribersByTopic[tp.topic] || []
    if (candidates.length === 0) continue

    let best = null
    let bestCount = Infinity
    for (const memberId of candidates) {
      const count = assignment[memberId] ? assignment[memberId].length : 0
      if (count < bestCount || (count === bestCount && (best === null || memberId < best))) {
        best = memberId
        bestCount = count
      }
    }

    if (!assignment[best]) assignment[best] = []
    assignment[best].push(tp)
  }
}

/**
 * Bounded rebalance pass. While some over-loaded member has ≥2 more partitions
 * than an under-loaded member that subscribes to one of its (preferably
 * non-owned) partitions, move a single partition. Hard iteration cap prevents
 * an infinite loop.
 *
 * @param {Object<string, Array<{topic: string, partition: number}>>} assignment (mutated)
 * @param {Object<string, string[]>} subscribersByTopic
 * @param {Set<string>} ownedKeys partitionKey() values seeded from stickiness (prefer NOT to move these)
 * @param {number} totalPartitions used as the iteration cap
 */
const rebalance = (assignment, subscribersByTopic, ownedKeys, totalPartitions) => {
  const memberIds = Object.keys(assignment).sort()
  let iterations = 0
  const maxIterations = totalPartitions * memberIds.length + totalPartitions + 1

  while (iterations++ < maxIterations) {
    const counts = memberIds.map(id => ({ id, count: assignment[id].length }))
    counts.sort((a, b) => b.count - a.count || (a.id < b.id ? -1 : 1))

    const over = counts[0]
    // Find the least-loaded member overall as the move target candidate pool.
    const under = counts[counts.length - 1]

    if (over.count - under.count <= 1) break

    const moved = moveOnePartition(over.id, assignment, subscribersByTopic, ownedKeys)
    if (!moved) break
  }
}

/**
 * Move one partition from `fromId` to a subscribed member that has fewer
 * partitions (by ≥2, so the move strictly improves balance). Prefer moving a
 * non-owned partition to preserve stickiness.
 *
 * @returns {boolean} whether a move happened
 */
const moveOnePartition = (fromId, assignment, subscribersByTopic, ownedKeys) => {
  const fromPartitions = assignment[fromId]
  const fromCount = fromPartitions.length

  // Prefer non-owned partitions first to preserve stickiness.
  const ordered = [...fromPartitions].sort((a, b) => {
    const aOwned = ownedKeys.has(partitionKey(a)) ? 1 : 0
    const bOwned = ownedKeys.has(partitionKey(b)) ? 1 : 0
    return aOwned - bOwned
  })

  for (const tp of ordered) {
    const candidates = (subscribersByTopic[tp.topic] || []).filter(id => id !== fromId)
    let best = null
    let bestCount = Infinity
    for (const memberId of candidates) {
      const count = assignment[memberId] ? assignment[memberId].length : 0
      if (count < bestCount || (count === bestCount && (best === null || memberId < best))) {
        best = memberId
        bestCount = count
      }
    }

    if (best !== null && fromCount - bestCount >= 2) {
      // Remove tp from source, add to target.
      const idx = fromPartitions.findIndex(
        p => p.topic === tp.topic && p.partition === tp.partition
      )
      fromPartitions.splice(idx, 1)
      if (!assignment[best]) assignment[best] = []
      assignment[best].push(tp)
      return true
    }
  }

  return false
}

module.exports = { fillFreePartitions, rebalance }
