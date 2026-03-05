const Encoder = require('../../../encoder')
const { ShareGroupHeartbeat: apiKey } = require('../../apiKeys')

// ShareGroupHeartbeat Request v0 => group_id member_id member_epoch rack_id [subscribed_topic_names] [topic_partitions] TAG_BUFFER
module.exports = ({
  groupId,
  memberId,
  memberEpoch,
  rackId = null,
  subscribedTopicNames = null,
  topicPartitions = null,
}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ShareGroupHeartbeat',
  encode: async () => {
    const encoder = new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(groupId)
      .writeUVarIntString(memberId)
      .writeInt32(memberEpoch)
      .writeUVarIntString(rackId)

    // subscribedTopicNames - COMPACT_NULLABLE_ARRAY of COMPACT_STRING
    if (subscribedTopicNames === null) {
      encoder.writeUVarInt(0)
    } else {
      encoder.writeUVarInt(subscribedTopicNames.length + 1)
      subscribedTopicNames.forEach(name => encoder.writeUVarIntString(name))
    }

    // topicPartitions - COMPACT_NULLABLE_ARRAY
    if (topicPartitions === null) {
      encoder.writeUVarInt(0)
    } else {
      encoder.writeUVarInt(topicPartitions.length + 1)
      topicPartitions.forEach(({ topicId, partitions }) => {
        encoder.writeUUID(topicId)
        // partitions - COMPACT_ARRAY of INT32
        encoder.writeUVarInt(partitions.length + 1)
        partitions.forEach(p => encoder.writeInt32(p))
        encoder.writeUVarIntBytes() // struct TAG_BUFFER
      })
    }

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
