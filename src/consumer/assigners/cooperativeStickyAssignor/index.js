const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const { UserData, DEFAULT_GENERATION } = require('./userData')
const { stickyAssign } = require('./stickyAssign')
const { computeTransferring, adjust } = require('./cooperative')

/**
 * CooperativeStickyAssignor
 *
 * Incremental cooperative rebalancing (KIP-429) + sticky assignment for
 * KafkaJS consumers, ported from Apache Kafka's CooperativeStickyAssignor.
 *
 * - Sticky: minimizes partition movement across rebalances while keeping the
 *   group balanced (max 1-partition skew).
 * - Cooperative: members keep partitions across a rebalance and only revoke
 *   what must move, converging in ≤2 rounds.
 *
 * Interop limitation (same as roundRobin): ownedPartitions + generationId are
 * encoded into the assigner `userData` buffer because KafkaJS has no native
 * ownedPartitions wire field. Therefore this assigner interoperates only within
 * KafkaJS groups, not mixed Java/KafkaJS groups.
 *
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'cooperative-sticky',
  version: 0,
  rebalanceProtocol: 'cooperative',

  /**
   * Run on the leader only. Decodes each member's metadata, computes the ideal
   * sticky assignment, strips transferring partitions (cooperative adjustment),
   * and encodes the per-member assignment buffers.
   *
   * @param {object} group
   * @param {import('types').GroupMember[]} group.members
   * @param {string[]} group.topics
   * @returns {Promise<import('types').GroupMemberAssignment[]>}
   */
  async assign({ members, topics }) {
    const subscriptions = members.map(({ memberId, memberMetadata }) => {
      const { topics: memberTopics, userData } = MemberMetadata.decode(memberMetadata)
      const { ownedPartitions, generationId } = UserData.decode(userData)
      return { memberId, topics: memberTopics, ownedPartitions, generationId }
    })

    const topicPartitions = {}
    for (const topic of topics) {
      topicPartitions[topic] = cluster.findTopicPartitionMetadata(topic).map(m => m.partitionId)
    }

    const ideal = stickyAssign({ members: subscriptions, topicPartitions })
    const transferring = computeTransferring(subscriptions, ideal)
    const adjusted = adjust(ideal, transferring)

    return Object.keys(adjusted).map(memberId => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: toAssignmentMap(adjusted[memberId]),
      }),
    }))
  },

  /**
   * Run on every member to build its JoinGroup metadata. ownedPartitions +
   * generationId are threaded through by the consumer group (Phase 4); they
   * default so the assigner works standalone / in tests.
   */
  protocol({ topics, ownedPartitions = {}, generationId = DEFAULT_GENERATION }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
        userData: UserData.encode({ generationId, ownedPartitions }),
      }),
    }
  },
})

/**
 * Convert a flat partition list into the { topic: number[] } shape that
 * MemberAssignment.encode expects.
 *
 * @param {Array<{topic: string, partition: number}>} partitions
 * @returns {Object<string, number[]>}
 */
const toAssignmentMap = partitions => {
  const map = Object.create(null)
  for (const { topic, partition } of partitions) {
    if (!map[topic]) map[topic] = []
    map[topic].push(partition)
  }
  for (const topic of Object.keys(map)) map[topic].sort((a, b) => a - b)
  return map
}
