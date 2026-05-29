const { partitionKey } = require('./partitionKey')

/**
 * Layer-1 conflict detection: resolve partitions claimed (owned) by more than
 * one member.
 *
 * Stickiness seeds each member with the partitions it currently owns, but two
 * members can advertise the same partition across a rebalance gap (e.g. a
 * partition mid-transfer). We resolve those dual-claims by generation:
 *   - higher `generationId` wins (it was assigned more recently)
 *   - equal generation → revoke from BOTH (the partition becomes free and is
 *     re-assigned by the balance pass)
 *
 * Only partitions that still exist in `validPartitionKeys` (current cluster
 * metadata, see S4) and whose topic the member still subscribes to are
 * considered retained.
 *
 * @param {Array<{ memberId: string, topics: string[], ownedPartitions: Object<string, number[]>, generationId: number }>} members
 * @param {Set<string>} validPartitionKeys partitionKey() values that exist in current metadata
 * @returns {{ retained: Object<string, Array<{topic: string, partition: number}>>, claimedKeys: Set<string> }}
 *   retained: memberId → list of owned partitions it keeps after conflict resolution
 *   claimedKeys: partitionKey() values that are retained by some member (not free)
 */
const resolveConflicts = (members, validPartitionKeys) => {
  // Find the best (highest-generation) claimant per partition.
  const bestClaim = new Map() // key -> { memberId, generationId, topic, partition, conflict }

  for (const member of members) {
    const subscribed = new Set(member.topics)

    for (const topic of Object.keys(member.ownedPartitions)) {
      if (!subscribed.has(topic)) continue

      for (const partition of member.ownedPartitions[topic]) {
        const key = partitionKey({ topic, partition })
        if (!validPartitionKeys.has(key)) continue

        const existing = bestClaim.get(key)
        if (!existing) {
          bestClaim.set(key, {
            memberId: member.memberId,
            generationId: member.generationId,
            topic,
            partition,
            conflict: false,
          })
        } else if (member.generationId > existing.generationId) {
          existing.memberId = member.memberId
          existing.generationId = member.generationId
          existing.conflict = false
        } else if (member.generationId === existing.generationId) {
          // Equal generation dual-claim → free the partition.
          existing.conflict = true
        }
        // Lower generation: ignore (loses to existing claim).
      }
    }
  }

  // Build retained map from non-conflicting winners.
  const retained = {}
  const claimedKeys = new Set()

  for (const [key, claim] of bestClaim) {
    if (claim.conflict) continue
    if (!retained[claim.memberId]) retained[claim.memberId] = []
    retained[claim.memberId].push({ topic: claim.topic, partition: claim.partition })
    claimedKeys.add(key)
  }

  return { retained, claimedKeys }
}

module.exports = { resolveConflicts }
