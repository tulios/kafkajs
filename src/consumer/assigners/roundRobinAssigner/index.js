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
  async assign({ members, topics }) {
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignment = {}

    // Decode per-member topic subscriptions from member metadata
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
      memberSubscriptions[memberId] = topics
    }

    for (const topic of topics) {
      const eligibleMembers = sortedMembers.filter(memberId =>
        memberSubscriptions[memberId].includes(topic)
      )

      if (eligibleMembers.length === 0) continue

      const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
      partitionMetadata.forEach((m, i) => {
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
