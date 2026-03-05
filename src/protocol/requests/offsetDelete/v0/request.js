const Encoder = require('../../../encoder')
const { OffsetDelete: apiKey } = require('../../apiKeys')

/**
 * OffsetDelete Request (Version: 0) => group_id [topics]
 *   group_id => STRING
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index
 *       partition_index => INT32
 */
module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'OffsetDelete',
  encode: async () => {
    return new Encoder().writeString(groupId).writeArray(
      topics.map(({ topic, partitions }) => {
        return new Encoder()
          .writeString(topic)
          .writeArray(partitions.map(({ partition }) => new Encoder().writeInt32(partition)))
      })
    )
  },
})
