const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')

/**
 * OffsetCommit Response (Version: 6) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeResponses)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
  }
}

const decodeResponses = decoder => {
  const response = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }
  decoder.readTaggedFields()
  return response
}

const decodePartitions = decoder => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return partition
}

module.exports = {
  decode,
  parse: parseV0,
}
