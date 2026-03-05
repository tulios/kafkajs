const Encoder = require('../../../encoder')
const { ShareFetch: apiKey } = require('../../apiKeys')

module.exports = ({
  groupId,
  memberId,
  memberEpoch,
  maxWaitMs = 500,
  minBytes = 1,
  maxBytes = 10485760,
  topics = [],
  forgottenTopicsData = [],
}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ShareFetch',
  encode: async () => {
    const encoder = new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(groupId)
      .writeUVarIntString(memberId)
      .writeInt32(memberEpoch)
      .writeInt32(maxWaitMs)
      .writeInt32(minBytes)
      .writeInt32(maxBytes)

    // topics - COMPACT_ARRAY
    encoder.writeUVarInt(topics.length + 1)
    topics.forEach(({ topicId, partitions }) => {
      encoder.writeUUID(topicId)
      encoder.writeUVarInt(partitions.length + 1)
      partitions.forEach(({ partitionIndex, partitionMaxBytes, acknowledgementBatches }) => {
        encoder.writeInt32(partitionIndex)
        encoder.writeInt32(partitionMaxBytes || 1048576)
        // acknowledgementBatches - COMPACT_ARRAY
        if (acknowledgementBatches && acknowledgementBatches.length > 0) {
          encoder.writeUVarInt(acknowledgementBatches.length + 1)
          acknowledgementBatches.forEach(({ firstOffset, lastOffset, acknowledgeTypes }) => {
            encoder.writeInt64(firstOffset)
            encoder.writeInt64(lastOffset)
            // acknowledgeTypes - COMPACT_ARRAY of INT8
            encoder.writeUVarInt(acknowledgeTypes.length + 1)
            acknowledgeTypes.forEach(t => encoder.writeInt8(t))
            encoder.writeUVarIntBytes() // struct TAG_BUFFER
          })
        } else {
          encoder.writeUVarInt(1) // empty array
        }
        encoder.writeUVarIntBytes() // struct TAG_BUFFER
      })
      encoder.writeUVarIntBytes() // struct TAG_BUFFER
    })

    // forgottenTopicsData - COMPACT_ARRAY
    encoder.writeUVarInt(forgottenTopicsData.length + 1)
    forgottenTopicsData.forEach(({ topicId, partitions }) => {
      encoder.writeUUID(topicId)
      encoder.writeUVarInt(partitions.length + 1)
      partitions.forEach(p => encoder.writeInt32(p))
      encoder.writeUVarIntBytes() // struct TAG_BUFFER
    })

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
