const Encoder = require('../../../encoder')
const { DescribeProducers: apiKey } = require('../../apiKeys')

/**
 * DescribeProducers Request (Version: 0) => TAG_BUFFER [topics] TAG_BUFFER
 * topics => name [partition_indexes] TAG_BUFFER
 *  name => COMPACT_STRING
 *  partition_indexes => INT32
 */

module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeProducers',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(topics.map(encodeTopics))
      .writeUVarIntBytes()
  },
})

const encodeTopics = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeUVarIntBytes()
}

const encodePartition = partition => {
  return new Encoder().writeInt32(partition)
}
