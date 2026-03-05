const Encoder = require('../../../encoder')
const { CreateTopics: apiKey } = require('../../apiKeys')

/**
 * CreateTopics Request (Version: 5) => [topics] timeout_ms validate_only TAG_BUFFER
 *   topics => name num_partitions replication_factor [assignments] [configs] TAG_BUFFER
 *     name => COMPACT_STRING
 *     num_partitions => INT32
 *     replication_factor => INT16
 *     assignments => partition_index [broker_ids] TAG_BUFFER
 *       partition_index => INT32
 *       broker_ids => INT32
 *     configs => name value TAG_BUFFER
 *       name => COMPACT_STRING
 *       value => COMPACT_NULLABLE_STRING
 *   timeout_ms => INT32
 *   validate_only => BOOLEAN
 */

module.exports = ({ topics, validateOnly = false, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'CreateTopics',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(topics.map(encodeTopics))
      .writeInt32(timeout)
      .writeBoolean(validateOnly)
      .writeUVarIntBytes()
  },
})

const encodeTopics = ({
  topic,
  numPartitions = -1,
  replicationFactor = -1,
  replicaAssignment = [],
  configEntries = [],
}) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeInt32(numPartitions)
    .writeInt16(replicationFactor)
    .writeUVarIntArray(replicaAssignment.map(encodeReplicaAssignment))
    .writeUVarIntArray(configEntries.map(encodeConfigEntries))
    .writeUVarIntBytes()
}

const encodeReplicaAssignment = ({ partition, replicas }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeUVarIntArray(replicas.map(r => new Encoder().writeInt32(r)))
    .writeUVarIntBytes()
}

const encodeConfigEntries = ({ name, value }) => {
  return new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntString(value)
    .writeUVarIntBytes()
}
