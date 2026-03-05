const Encoder = require('../../../encoder')
const { DescribeLogDirs: apiKey } = require('../../apiKeys')

/**
 * DescribeLogDirs Request (Version: 0) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 */

module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeLogDirs',
  encode: async () => {
    return new Encoder().writeArray(
      topics
        ? topics.map(({ topic, partitions }) => {
            return new Encoder()
              .writeString(topic)
              .writeArray(partitions.map(partition => new Encoder().writeInt32(partition)))
          })
        : [],
      undefined,
      topics ? undefined : -1
    )
  },
})
