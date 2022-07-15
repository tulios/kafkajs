const Encoder = require('../../../encoder')
const { AlterPartitionReassignments: apiKey } = require('../../apiKeys')

/**
 * AlterPartitionReassignments Request (Version: 0) => timeout_ms [topics] TAG_BUFFER
 * timeout_ms => INT32
 * topics => name [partitions] TAG_BUFFER
 *  name => COMPACT_STRING
 *  partitions => partition_index [replicas] TAG_BUFFER
 *    partition_index => INT32
 *    replicas => INT32
 */

module.exports = ({ topics, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'AlterPartitionReassignments',
  encode: async () => {
    return new Encoder().writeInt32(timeout).writeArray(topics.map(encodeTopics))
  },
})

const encodeTopics = ({ topic, partitionAssignment }) => {
  return new Encoder()
    .writeString(topic)
    .writeArray(partitionAssignment.map(encodePartitionAssignment))
}

const encodePartitionAssignment = ({ partition, replicas }) => {
  return new Encoder().writeInt32(partition).writeArray(replicas)
}
