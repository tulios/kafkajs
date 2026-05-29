const { partitionKey } = require('./partitionKey')
const { resolveConflicts } = require('./conflicts')
const { fillFreePartitions, rebalance } = require('./balance')

/**
 * Pure sticky assignment core.
 *
 * Given members (each with subscribed topics + ownedPartitions + generationId)
 * and the cluster's partition metadata, produce a balanced assignment
 * (max 1-partition skew within a subscription group) that preserves as many
 * owned partitions as possible.
 *
 * This is the eager-equivalent sticky result; cooperative adjustment (Phase 3)
 * layers on top. No KafkaJS consumer-flow coupling — testable as a pure
 * function.
 *
 * @param {Object} params
 * @param {Array<{ memberId: string, topics: string[], ownedPartitions: Object<string, number[]>, generationId: number }>} params.members
 * @param {Object<string, number[]>} params.topicPartitions topic → partition ids that currently exist in metadata
 * @returns {Object<string, Array<{topic: string, partition: number}>>} memberId → assigned partitions
 */
const stickyAssign = ({ members, topicPartitions }) => {
  const sortedMembers = [...members].sort((a, b) => (a.memberId < b.memberId ? -1 : 1))

  // topic → sorted subscriber memberIds
  const subscribersByTopic = {}
  for (const member of sortedMembers) {
    for (const topic of member.topics) {
      if (!subscribersByTopic[topic]) subscribersByTopic[topic] = []
      subscribersByTopic[topic].push(member.memberId)
    }
  }

  // Full list of partitions that exist AND have at least one subscriber.
  // (S4) Only partitions present in current metadata are assignable; owned
  // partitions for vanished/shrunk topics are dropped here implicitly.
  const allPartitions = []
  const validPartitionKeys = new Set()
  for (const topic of Object.keys(topicPartitions).sort()) {
    if (!subscribersByTopic[topic]) continue
    for (const partition of [...topicPartitions[topic]].sort((a, b) => a - b)) {
      const tp = { topic, partition }
      allPartitions.push(tp)
      validPartitionKeys.add(partitionKey(tp))
    }
  }

  // Step 2: seed from owned (stickiness) with Layer-1 conflict resolution.
  const { retained, claimedKeys } = resolveConflicts(sortedMembers, validPartitionKeys)

  const assignment = {}
  for (const member of sortedMembers) {
    assignment[member.memberId] = retained[member.memberId] ? [...retained[member.memberId]] : []
  }

  // Step 3: collect free partitions (valid partitions not retained by anyone).
  const freePartitions = allPartitions.filter(tp => !claimedKeys.has(partitionKey(tp)))

  // Step 4: greedy fill of free partitions to least-loaded subscribed member.
  fillFreePartitions(freePartitions, assignment, subscribersByTopic)

  // Step 5: bounded rebalance pass (prefer moving non-owned partitions).
  rebalance(assignment, subscribersByTopic, claimedKeys, allPartitions.length)

  // Step 6: Layer-2 invariant — every partition assigned exactly once.
  assertAssignedOnce(assignment, allPartitions)

  return assignment
}

/**
 * Invariant assertion: each valid partition appears exactly once across the
 * assignment, and no extra partitions leaked in.
 */
const assertAssignedOnce = (assignment, allPartitions) => {
  const seen = new Set()
  for (const memberId of Object.keys(assignment)) {
    for (const tp of assignment[memberId]) {
      const key = partitionKey(tp)
      if (seen.has(key)) {
        throw new Error(`stickyAssign invariant violated: ${key} assigned more than once`)
      }
      seen.add(key)
    }
  }

  if (seen.size !== allPartitions.length) {
    throw new Error(
      `stickyAssign invariant violated: assigned ${seen.size} partitions, expected ${allPartitions.length}`
    )
  }
}

module.exports = { stickyAssign }
