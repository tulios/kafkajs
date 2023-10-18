const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * RangeAssigner
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'RangeAssigner',
  version: 0,

  /**
   * Assign the topics to the provided members using range strategy.
   *
   * The members array contains information about each member, `memberMetadata` is the result of the
   * `protocol` operation.
   *
   * @param {object} group
   * @param {import('types').GroupMember[]} group.members array of members, e.g:
                              [{ memberId: 'test-5f93f5a3', memberMetadata: Buffer }]
   * @param {string[]} group.topics
   * @returns {Promise<import('types').GroupMemberAssignment[]>} object partitions per topic per member
   */
  async assign({ members, topics }) {
    // logic inspired from kafka java client:
    // https://kafka.apache.org/24/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html
    // https://github.com/a0x8o/kafka/blob/master/clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java

    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const membersCount = sortedMembers.length
    const assignment = {}

    for (const topic of topics) {
      const partitionMetadata = cluster.findTopicPartitionMetadata(topic)

      const numPartitionsForTopic = partitionMetadata.length
      const numPartitionsPerConsumer = Math.floor(numPartitionsForTopic / membersCount)
      const consumersWithExtraPartition = numPartitionsForTopic % membersCount

      for (var i = 0; i < membersCount; i++) {
        const start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition)
        const length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1)

        const assignee = sortedMembers[i]

        for (let partition = start; partition < start + length; partition++) {
          if (!assignment[assignee]) {
            assignment[assignee] = Object.create(null)
          }

          if (!assignment[assignee][topic]) {
            assignment[assignee][topic] = []
          }

          assignment[assignee][topic].push(partition)
        }
      }
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
