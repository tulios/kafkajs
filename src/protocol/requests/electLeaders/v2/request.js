const Encoder = require('../../../encoder')
const { ElectPreferredLeaders: apiKey } = require('../../apiKeys')

/**
 * ElectLeaders Request (Version: 2) => election_type [topic_partitions] timeout TAG_BUFFER
 *   election_type => INT8
 *   topic_partitions => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => INT32
 *   timeout => INT32
 */
module.exports = ({ electionType = 0, topicPartitions, timeout = 30000 }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'ElectLeaders',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeInt8(electionType)
      .writeUVarIntArray(
        topicPartitions
          ? topicPartitions.map(({ topic, partitions }) => {
              return new Encoder()
                .writeUVarIntString(topic)
                .writeUVarIntArray(partitions.map(partition => new Encoder().writeInt32(partition)))
                .writeUVarIntBytes()
            })
          : null
      )
      .writeInt32(timeout)
      .writeUVarIntBytes()
  },
})
