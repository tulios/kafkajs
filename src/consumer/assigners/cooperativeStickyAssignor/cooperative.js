const { partitionKey } = require('./partitionKey')

/**
 * Cooperative (KIP-429) two-phase adjustment.
 *
 * The Phase-2 sticky result is the *ideal* eager assignment. To preserve the
 * cooperative invariant — no partition is owned by two members across the
 * rebalance gap — we must not hand a partition to its new owner while the old
 * owner still holds it. Such partitions are "transferring": they are stripped
 * from this round's assignment, the old owner revokes them on SYNC, and the
 * freed partition is picked up in the next round (Phase 4 drives the rejoin).
 */

/**
 * Build a memberId → Set(partitionKey) map of what each member currently owns,
 * from the decoded subscriptions.
 *
 * @param {Array<{ memberId: string, ownedPartitions: Object<string, number[]> }>} subscriptions
 * @returns {Map<string, Set<string>>}
 */
const buildOwnedIndex = subscriptions => {
  const owned = new Map()
  for (const sub of subscriptions) {
    const keys = new Set()
    for (const topic of Object.keys(sub.ownedPartitions || {})) {
      for (const partition of sub.ownedPartitions[topic]) {
        keys.add(partitionKey({ topic, partition }))
      }
    }
    owned.set(sub.memberId, keys)
  }
  return owned
}

/**
 * Compute the set of partitions that are transferring owner: present in the
 * ideal assignment for some member, but still owned by a *different* member.
 *
 * @param {Array<{ memberId: string, ownedPartitions: Object<string, number[]> }>} subscriptions
 * @param {Object<string, Array<{topic: string, partition: number}>>} idealAssignment memberId → partitions
 * @returns {Set<string>} partitionKey() values that are transferring
 */
const computeTransferring = (subscriptions, idealAssignment) => {
  const ownedIndex = buildOwnedIndex(subscriptions)
  const transferring = new Set()

  for (const newOwner of Object.keys(idealAssignment)) {
    for (const tp of idealAssignment[newOwner]) {
      const key = partitionKey(tp)
      // Is some OTHER member still claiming this partition?
      for (const [memberId, ownedKeys] of ownedIndex) {
        if (memberId !== newOwner && ownedKeys.has(key)) {
          transferring.add(key)
          break
        }
      }
    }
  }

  return transferring
}

/**
 * Strip transferring partitions from the ideal assignment. The result is the
 * assignment to return for this cooperative round; transferring partitions are
 * withheld from their new owner until the old owner revokes them.
 *
 * @param {Object<string, Array<{topic: string, partition: number}>>} idealAssignment
 * @param {Set<string>} transferring
 * @returns {Object<string, Array<{topic: string, partition: number}>>}
 */
const adjust = (idealAssignment, transferring) => {
  const adjusted = {}
  for (const memberId of Object.keys(idealAssignment)) {
    adjusted[memberId] = idealAssignment[memberId].filter(tp => !transferring.has(partitionKey(tp)))
  }
  return adjusted
}

module.exports = { computeTransferring, adjust }
