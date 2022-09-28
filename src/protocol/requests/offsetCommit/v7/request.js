const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

// OffsetCommit Request (Version: 7) => group_id generation_id member_id group_instance_id [topics] 
//   group_id => STRING
//   generation_id => INT32
//   member_id => STRING
//   group_instance_id => NULLABLE_STRING
//   topics => name [partitions] 
//     name => STRING
//     partitions => partition_index committed_offset committed_leader_epoch committed_metadata 
//       partition_index => INT32
//       committed_offset => INT64
//       committed_leader_epoch => INT32
//       committed_metadata => NULLABLE_STRING

module.exports = ({ groupId, groupGenerationId, memberId, groupInstanceId, topics }) => ({
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

const encodePartition = ({ partition, offset, leaderEpoch, metadata = null }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch)
    .writeString(metadata)
}
