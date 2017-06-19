const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')

/**
 * Metadata Response (Version: 0) => [brokers] [topic_metadata]
 *   brokers => node_id host port
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *   topic_metadata => topic_error_code topic [partition_metadata]
 *     topic_error_code => INT16
 *     topic => STRING
 *     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
 *       partition_error_code => INT16
 *       partition_id => INT32
 *       leader => INT32
 *       replicas => INT32
 *       isr => INT32
 */

const broker = decoder => ({
  nodeId: decoder.readInt32(),
  host: decoder.readString(),
  port: decoder.readInt32(),
})

const topicMetadata = decoder => ({
  topicErrorCode: decoder.readInt16(),
  topic: decoder.readString(),
  partitionMetadata: decoder.readArray(partitionMetadata),
})

const partitionMetadata = decoder => ({
  partitionErrorCode: decoder.readInt16(),
  partitionId: decoder.readInt32(),
  leader: decoder.readInt32(),
  replicas: decoder.readInt32(),
  isr: decoder.readInt32(),
})

const decode = rawData => {
  const decoder = new Decoder(rawData)
  return {
    brokers: decoder.readArray(broker),
    topicMetadata: decoder.readArray(topicMetadata),
  }
}

const flatten = arrays => [].concat.apply([], arrays)

const parse = data => {
  const topicsWithErrors = data.topicMetadata.filter(topic => failure(topic.topicErrorCode))
  if (topicsWithErrors.length > 0) {
    const { topicErrorCode } = topicsWithErrors[0]
    throw new KafkaProtocolError(topicErrorCode)
  }

  const partitionsWithErrors = data.topicMetadata.map(topic => {
    return topic.partitionMetadata.filter(partition => failure(partition.partitionErrorCode))
  })

  const errors = flatten(partitionsWithErrors)
  if (errors.length > 0) {
    const { partitionErrorCode } = error[0]
    throw new KafkaProtocolError(partitionErrorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
