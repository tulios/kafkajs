const Encoder = require('../../../encoder')
const { DeleteConsumerGroupOffsets: apiKey } = require('../../apiKeys')

/**
 * DeleteConsumerGroupOffsets Request (Version: 0) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 */

module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DeleteConsumerGroupOffsets',
  encode: async () => {
    return new Encoder().writeString(groupId).writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeString(topic)
    .writeArray(partitions.map(p => new Encoder().writeInt32(p)))
}
