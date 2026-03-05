const Encoder = require('../../../encoder')
const { Fetch: apiKey } = require('../../apiKeys')
const ISOLATION_LEVEL = require('../../../isolationLevel')

/**
 * Fetch Request (Version: 12) => replica_id max_wait_time min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
 *   replica_id => INT32
 *   max_wait_time => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   isolation_level => INT8
 *   session_id => INT32
 *   session_epoch => INT32
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       fetch_offset => INT64
 *       last_fetched_epoch => INT32
 *       log_start_offset => INT64
 *       partition_max_bytes => INT32
 *   forgotten_topics_data => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => INT32
 *   rack_id => COMPACT_STRING
 */

module.exports = ({
  replicaId,
  maxWaitTime,
  minBytes,
  maxBytes,
  topics,
  rackId = '',
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  sessionId = 0,
  sessionEpoch = -1,
  forgottenTopics = [], // Topics to remove from the fetch session
}) => ({
  apiKey,
  apiVersion: 12,
  apiName: 'Fetch',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeInt32(replicaId)
      .writeInt32(maxWaitTime)
      .writeInt32(minBytes)
      .writeInt32(maxBytes)
      .writeInt8(isolationLevel)
      .writeInt32(sessionId)
      .writeInt32(sessionEpoch)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeUVarIntArray(forgottenTopics.map(encodeForgottenTopics))
      .writeUVarIntString(rackId)
      .writeUVarIntBytes()
  },
})

const encodeForgottenTopics = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(p => new Encoder().writeInt32(p)))
    .writeUVarIntBytes()
}

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeUVarIntBytes()
}

const encodePartition = ({
  partition,
  currentLeaderEpoch = -1,
  fetchOffset,
  lastFetchedEpoch = -1,
  logStartOffset = -1,
  maxBytes,
}) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt64(fetchOffset)
    .writeInt32(lastFetchedEpoch)
    .writeInt64(logStartOffset)
    .writeInt32(maxBytes)
    .writeUVarIntBytes()
}
