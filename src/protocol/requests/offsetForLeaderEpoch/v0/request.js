const Encoder = require('../../../encoder')
const { OffsetForLeaderEpoch: apiKey } = require('../../apiKeys')

/**
 * OffsetsForLeaderEpoch Request (Version: 0) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition leader_epoch
 *       partition => INT32
 *       leader_epoch => INT32
 */
module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'OffsetForLeaderEpoch',
  encode: async () => {
    return new Encoder().writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({ partition, leaderEpoch }) => {
  return new Encoder().writeInt32(partition).writeInt32(leaderEpoch)
}
