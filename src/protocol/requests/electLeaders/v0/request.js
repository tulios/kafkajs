const Encoder = require('../../../encoder')
const { ElectPreferredLeaders: apiKey } = require('../../apiKeys')

/**
 * ElectLeaders Request (Version: 0) => election_type [topic_partitions] timeout
 *   election_type => INT8
 *   topic_partitions => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 *   timeout => INT32
 */
module.exports = ({ electionType = 0, topicPartitions, timeout = 30000 }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ElectLeaders',
  encode: async () => {
    return new Encoder()
      .writeInt8(electionType)
      .writeArray(
        topicPartitions
          ? topicPartitions.map(({ topic, partitions }) => {
              return new Encoder()
                .writeString(topic)
                .writeArray(partitions.map(partition => new Encoder().writeInt32(partition)))
            })
          : null
      )
      .writeInt32(timeout)
  },
})
