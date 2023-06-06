const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * Version 7 group_instance_id added
 *
 * OffsetCommit Request (Version: 7) => group_id generation_id member_id [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING //new
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 */

module.exports = ({ groupId, groupGenerationId, groupInstanceId = null, memberId, topics }) => ({
  apiKey,
  apiVersion: 7,
  apiName: 'OffsetCommit',
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeInt32(groupGenerationId)
      .writeString(memberId)
      .writeString(groupInstanceId)
      .writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({ partition, offset, committedLeaderEpoch, metadata = null }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(committedLeaderEpoch)
    .writeString(metadata)
}
