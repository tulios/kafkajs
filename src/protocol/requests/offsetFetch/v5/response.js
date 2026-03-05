const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * OffsetFetch Response (Version: 5) => throttle_time_ms [topics] error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index committed_offset committed_leader_epoch metadata error_code TAG_BUFFER
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => COMPACT_NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeResponses)
  const errorCode = decoder.readInt16()
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
    errorCode,
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
    offset: decoder.readInt64().toString(),
    committedLeaderEpoch: decoder.readInt32(),
    metadata: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return partition
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  const partitionsWithError = data.responses.flatMap(response =>
    response.partitions.filter(partition => failure(partition.errorCode))
  )
  const partitionWithError = partitionsWithError[0]
  if (partitionWithError) {
    throw createErrorFromCode(partitionWithError.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
