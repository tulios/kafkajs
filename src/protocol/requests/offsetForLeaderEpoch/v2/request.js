const Encoder = require('../../../encoder')
const { OffsetForLeaderEpoch: apiKey } = require('../../apiKeys')

/**
 * OffsetsForLeaderEpoch Request (Version: 2) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch leader_epoch
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       leader_epoch => INT32
 */
module.exports = ({ replicaId = -1, topics }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'OffsetForLeaderEpoch',
  encode: async () => {
    return new Encoder().writeInt32(replicaId).writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({ partition, currentLeaderEpoch, leaderEpoch }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt32(leaderEpoch)
}
