const Encoder = require('../../../encoder')
const { DescribeLogDirs: apiKey } = require('../../apiKeys')

/**
 * DescribeLogDirs Request (Version: 2) => [topics] TAG_BUFFER
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => INT32
 */

module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DescribeLogDirs',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(
        topics
          ? topics.map(({ topic, partitions }) => {
              return new Encoder()
                .writeUVarIntString(topic)
                .writeUVarIntArray(partitions.map(partition => new Encoder().writeInt32(partition)))
                .writeUVarIntBytes()
            })
          : null
      )
      .writeUVarIntBytes()
  },
})
