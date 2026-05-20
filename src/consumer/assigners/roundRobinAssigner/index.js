const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * RoundRobinAssigner
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'RoundRobinAssigner',
  version: 0,

  /**
   * Assign the topics to the provided members.
   *
   * The members array contains information about each member, `memberMetadata` is the result of the
   * `protocol` operation.
   *
   * @param {object} group
   * @param {import('types').GroupMember[]} group.members array of members, e.g:
                              [{ memberId: 'test-5f93f5a3', memberMetadata: Buffer }]
   * @param {string[]} group.topics
   * @returns {Promise<import('types').GroupMemberAssignment[]>} object partitions per topic per member, e.g:
   *                   [
   *                     {
   *                       memberId: 'test-5f93f5a3',
   *                       memberAssignment: {
   *                         'topic-A': [0, 2, 4, 6],
   *                         'topic-B': [1],
   *                       },
   *                     },
   *                     {
   *                       memberId: 'test-3d3d5341',
   *                       memberAssignment: {
   *                         'topic-A': [1, 3, 5],
   *                         'topic-B': [0, 2],
   *                       },
   *                     }
   *                   ]
   */
  async assign({ members, topics, allSubscribedTopics }) {
    const assignableTopics = allSubscribedTopics || topics
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignment = {}

    // NOTE:
    // Eligibility is topic-scoped (not partition-scoped).
    // MemberMetadata carries subscribed topics only, so we:
    //   1) build eligible members per topic
    //   2) distribute that topic's partitions across only those members
    // This keeps mixed-subscription rollouts stable (e.g. v1[A] + v2[A,B]).

    // Decode per-member topic subscriptions from member metadata.
    // If decode fails, fallback remains intentionally safe/non-throwing.
    const memberSubscriptions = {}
    for (const { memberId, memberMetadata } of members) {
      if (memberMetadata) {
        try {
          const decoded = MemberMetadata.decode(memberMetadata)
          if (decoded.topics && decoded.topics.length > 0) {
            memberSubscriptions[memberId] = decoded.topics
            continue
          }
        } catch (e) {}
      }
      // Fallback: member is treated as subscribed to all topics
      memberSubscriptions[memberId] = assignableTopics
    }

    for (const topic of assignableTopics) {
      // Topic-level filter: this is the correct abstraction for assigner eligibility.
      const eligibleMembers = sortedMembers.filter(memberId =>
        memberSubscriptions[memberId].includes(topic)
      )

      if (eligibleMembers.length === 0) continue

      const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
      partitionMetadata.forEach((m, i) => {
        // Round-robin partitions within the already-eligible member set.
        // No additional partition-level subscription checks are expected here.
        const assignee = eligibleMembers[i % eligibleMembers.length]

        if (!assignment[assignee]) {
          assignment[assignee] = Object.create(null)
        }

        if (!assignment[assignee][topic]) {
          assignment[assignee][topic] = []
        }

        assignment[assignee][topic].push(m.partitionId)
      })
    }

    return Object.keys(assignment).map(memberId => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
      }),
    }))
  },

  protocol({ topics }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
      }),
    }
  },
})
