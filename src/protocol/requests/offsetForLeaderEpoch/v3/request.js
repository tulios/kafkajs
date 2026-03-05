const Encoder = require('../../../encoder')
const { OffsetForLeaderEpoch: apiKey } = require('../../apiKeys')

/**
 * OffsetsForLeaderEpoch Request (Version: 3) => replica_id [topics] TAG_BUFFER
 *   replica_id => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition current_leader_epoch leader_epoch TAG_BUFFER
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       leader_epoch => INT32
 */
module.exports = ({ replicaId = -1, topics }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'OffsetForLeaderEpoch',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeInt32(replicaId)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeUVarIntBytes()
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeUVarIntBytes()
}

const encodePartition = ({ partition, currentLeaderEpoch, leaderEpoch }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt32(leaderEpoch)
    .writeUVarIntBytes()
}
