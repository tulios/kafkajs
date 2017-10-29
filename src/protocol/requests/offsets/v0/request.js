const Encoder = require('../../../encoder')
const { Offsets: apiKey } = require('../../apiKeys')

/**
 * Offsets Request (Version: 0) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition timestamp max_num_offsets
 *       partition => INT32
 *       timestamp => INT64
 *       max_num_offsets => INT32
 */

module.exports = ({ replicaId, topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'Offsets',
  encode: () => {
    return new Encoder().writeInt32(replicaId).writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({ partition, timestamp = Date.now(), maxNumOffsets = 1 }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt64(timestamp)
    .writeInt32(maxNumOffsets)
}
