const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * RoundRobinAssigner
 * @param {Cluster} cluster
 * @returns {function}
 */
module.exports = ({ cluster }) => ({
  name: 'RoundRobinAssigner',
  version: 1,

  /**
   * This process can result in imbalanced assignments
   * @param {array} members array of members, e.g:
                              [{ memberId: 'test-5f93f5a3' }]
   * @param {array} topics
   * @param {Buffer} userData
   * @returns {array} object partitions per topic per member, e.g:
   *                   [
   *                     {
   *                       memberId: 'test-5f93f5a3',
   *                       memberAssignment: {
   *                         'topic-A': [0, 2, 4, 6],
   *                         'topic-B': [0, 2],
   *                       },
   *                     },
   *                     {
   *                       memberId: 'test-3d3d5341',
   *                       memberAssignment: {
   *                         'topic-A': [1, 3, 5],
   *                         'topic-B': [1],
   *                       },
   *                     }
   *                   ]
   */
  async assign({ members, topics, userData }) {
    const membersCount = members.length
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignment = {}

    sortedMembers.forEach(memberId => {
      assignment[memberId] = {}
    })

    topics.forEach(topic => {
      const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
      const partitions = partitionMetadata.map(m => m.partitionId)
      sortedMembers.forEach((memberId, i) => {
        if (!assignment[memberId][topic]) {
          assignment[memberId][topic] = []
        }

        assignment[memberId][topic].push(...partitions.filter(id => id % membersCount === i))
      })
    })

    return Object.keys(assignment).map(memberId => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
        userData,
      }),
    }))
  },

  protocol({ topics, userData }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
        userData,
      }),
    }
  },
})
