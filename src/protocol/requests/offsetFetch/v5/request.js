const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 5) => group_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => name [partition_indexes] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partition_indexes => INT32
 */

module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'OffsetFetch',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(topics === null ? null : topics.map(encodeTopic))
      .writeUVarIntBytes()
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeUVarIntBytes()
}

const encodePartition = ({ partition }) => {
  return new Encoder().writeInt32(partition).writeUVarIntBytes()
}
