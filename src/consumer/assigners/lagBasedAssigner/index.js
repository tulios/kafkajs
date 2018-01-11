const Long = require('long')
const { assign, values, keys } = Object

const head = (arr = []) => arr[0]

const initializeAssignments = members =>
  members.reduce((assignments, memberId) => assign({}, { [memberId]: {} }, assignments), {})

const byOffsetLagDescending = ({ offsetLag: a }, { offsetLag: b }) => b - a

const offsetsForTopic = (offsets, targetTopic) =>
  offsets.find(({ topic }) => topic === targetTopic).partitions

const offsetForPartition = (offsets, targetPartition) =>
  offsets.find(({ partition }) => partition === targetPartition).offset

const mergePartitionOffsets = (consumerOffsets, latestOffsets) => {
  return consumerOffsets.map(({ topic, partitions: consumerOffsetForPartititions }) => {
    const latestOffsetsByPartition = offsetsForTopic(latestOffsets, topic)

    return {
      topic,
      partitions: consumerOffsetForPartititions.map(({ partition, offset }) => {
        const latestPartitionOffset = offsetForPartition(latestOffsetsByPartition, partition)
        const offsetLag = Long.fromValue(latestPartitionOffset).subtract(Long.fromValue(offset))

        return {
          partition,
          offsetLag,
        }
      }),
    }
  })
}

/**
 * LagBasedAssigner
 *
 * Assigns partitions to members based on offset lag - attempting to spread
 * it as evenly as possible.
 *
 * The difference in number of assigned partitions between the most assigned
 * member and the least assigned member should never be greater than 1.
 *
 * If the two members with the fewest number of partitions assigned have the same
 * number of assigned partitions, the one where the sum of the offset lag of the
 * assigned partitions is the lowest should be assigned the unassigned partition.
 *
 * @param {Cluster} cluster
 * @param {string} groupId
 * @returns {function}
 */
module.exports = ({ cluster, groupId, logger }) => {
  const calculateOffsetLag = async topics => {
    const topicsWithPartitions = topics.map(topic => ({
      topic,
      partitions: cluster
        .findTopicPartitionMetadata(topic)
        .map(({ partitionId: partition }) => ({ partition })),
    }))

    const latestOffsets = await cluster.fetchTopicsOffset(topicsWithPartitions)
    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const { responses: consumerOffsets } = await coordinator.offsetFetch({
      groupId,
      topics: topicsWithPartitions,
    })

    const offsetLagByPartition = mergePartitionOffsets(consumerOffsets, latestOffsets)

    return offsetLagByPartition
  }

  /**
   * @param {array} members array of members, e.g:
                                [{ memberId: 'test-5f93f5a3' }]
   * @param {array} topics
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
  return async ({ members, topics }) => {
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignments = initializeAssignments(sortedMembers)

    const assignMember = (memberId, topic, partitionId) => {
      if (!assignments[memberId][topic]) {
        assignments[memberId][topic] = []
      }
      assignments[memberId][topic].push(partitionId)
    }

    const assignedPartitions = memberId =>
      values(assignments[memberId] || {}).reduce(
        (assignedPartitions, partitionsForTopic) => assignedPartitions.concat(partitionsForTopic),
        []
      )

    const leastAssignedMember = members => {
      const numberOfAssignedPartitions = memberId => assignedPartitions(memberId).length
      const byNumberOfAssignedPartitions = (a, b) =>
        numberOfAssignedPartitions(a) - numberOfAssignedPartitions(b)

      return head(members.sort(byNumberOfAssignedPartitions))
    }

    const topicPartitionOffsetLag = await calculateOffsetLag(topics)

    topics.forEach(topic => {
      const partitions = topicPartitionOffsetLag
        .find(({ topic: topicWithLag }) => topicWithLag === topic)
        .partitions.sort(byOffsetLagDescending)

      partitions.forEach(({ partition, offsetLag }) =>
        assignMember(leastAssignedMember(sortedMembers), topic, partition)
      )
    })

    return keys(assignments).map(memberId => ({
      memberId,
      memberAssignment: assignments[memberId],
    }))
  }
}
