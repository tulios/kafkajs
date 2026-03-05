const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * OffsetCommit Request (Version: 7) => group_id generation_id member_id group_instance_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index committed_offset committed_leader_epoch committed_metadata TAG_BUFFER
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       committed_metadata => COMPACT_NULLABLE_STRING
 */

module.exports = ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
  apiKey,
  apiVersion: 7,
  apiName: 'OffsetCommit',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(groupId)
      .writeInt32(groupGenerationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeUVarIntBytes()
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeUVarIntBytes()
}

const encodePartition = ({ partition, offset, committedLeaderEpoch = -1, metadata = null }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(committedLeaderEpoch)
    .writeUVarIntString(metadata)
    .writeUVarIntBytes()
}
