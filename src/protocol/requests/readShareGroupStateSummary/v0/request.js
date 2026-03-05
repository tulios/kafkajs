const Encoder = require('../../../encoder')
const { ReadShareGroupStateSummary: apiKey } = require('../../apiKeys')

module.exports = ({ topics = [] }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ReadShareGroupStateSummary',
  encode: async () => {
    const encoder = new Encoder().writeUVarIntBytes() // header TAG_BUFFER

    // topics - COMPACT_ARRAY
    encoder.writeUVarInt(topics.length + 1)
    topics.forEach(({ groupId, topicId, partitions }) => {
      encoder.writeUVarIntString(groupId)
      encoder.writeUUID(topicId)
      // partitions - COMPACT_ARRAY
      encoder.writeUVarInt(partitions.length + 1)
      partitions.forEach(({ partitionIndex, leaderEpoch }) => {
        encoder.writeInt32(partitionIndex)
        encoder.writeInt32(leaderEpoch)
        encoder.writeUVarIntBytes() // struct TAG_BUFFER
      })
      encoder.writeUVarIntBytes() // struct TAG_BUFFER
    })

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
