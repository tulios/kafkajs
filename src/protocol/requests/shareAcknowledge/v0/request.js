const Encoder = require('../../../encoder')
const { ShareAcknowledge: apiKey } = require('../../apiKeys')

// ShareAcknowledge Request v0
// ACCEPT = 1, RELEASE = 2, REJECT = 3, GAP = 4
module.exports = ({ groupId, memberId, memberEpoch, topics = [] }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ShareAcknowledge',
  encode: async () => {
    const encoder = new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(groupId)
      .writeUVarIntString(memberId)
      .writeInt32(memberEpoch)

    // topics - COMPACT_ARRAY
    encoder.writeUVarInt(topics.length + 1)
    topics.forEach(({ topicId, partitions }) => {
      encoder.writeUUID(topicId)
      encoder.writeUVarInt(partitions.length + 1)
      partitions.forEach(({ partitionIndex, acknowledgementBatches }) => {
        encoder.writeInt32(partitionIndex)
        encoder.writeUVarInt(acknowledgementBatches.length + 1)
        acknowledgementBatches.forEach(({ firstOffset, lastOffset, acknowledgeTypes }) => {
          encoder.writeInt64(firstOffset)
          encoder.writeInt64(lastOffset)
          encoder.writeUVarInt(acknowledgeTypes.length + 1)
          acknowledgeTypes.forEach(t => encoder.writeInt8(t))
          encoder.writeUVarIntBytes() // struct TAG_BUFFER
        })
        encoder.writeUVarIntBytes() // struct TAG_BUFFER
      })
      encoder.writeUVarIntBytes() // struct TAG_BUFFER
    })

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
