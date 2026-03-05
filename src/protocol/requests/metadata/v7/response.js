const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')

/**
 * Metadata Response (Version: 7) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 *   throttle_time_ms => INT32
 *   brokers => node_id host port rack
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *     rack => NULLABLE_STRING
 *   cluster_id => NULLABLE_STRING
 *   controller_id => INT32
 *   topic_metadata => error_code topic is_internal [partition_metadata]
 *     error_code => INT16
 *     topic => STRING
 *     is_internal => BOOLEAN
 *     partition_metadata => error_code partition leader leader_epoch [replicas] [isr] [offline_replicas]
 *       error_code => INT16
 *       partition => INT32
 *       leader => INT32
 *       leader_epoch => INT32
 *       replicas => INT32
 *       isr => INT32
 *       offline_replicas => INT32
 */

const broker = decoder => ({
  nodeId: decoder.readInt32(),
  host: decoder.readString(),
  port: decoder.readInt32(),
  rack: decoder.readString(),
})

const topicMetadata = decoder => ({
  topicErrorCode: decoder.readInt16(),
  topic: decoder.readString(),
  isInternal: decoder.readBoolean(),
  partitionMetadata: decoder.readArray(partitionMetadata),
})

const partitionMetadata = decoder => ({
  partitionErrorCode: decoder.readInt16(),
  partitionId: decoder.readInt32(),
  leader: decoder.readInt32(),
  leaderEpoch: decoder.readInt32(),
  replicas: decoder.readArray(d => d.readInt32()),
  isr: decoder.readArray(d => d.readInt32()),
  offlineReplicas: decoder.readArray(d => d.readInt32()),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    brokers: decoder.readArray(broker),
    clusterId: decoder.readString(),
    controllerId: decoder.readInt32(),
    topicMetadata: decoder.readArray(topicMetadata),
  }
}

module.exports = {
  decode,
  parse: parseV0,
}
