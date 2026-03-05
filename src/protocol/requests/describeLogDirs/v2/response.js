const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * DescribeLogDirs Response (Version: 2) => TAG_BUFFER throttle_time [results] TAG_BUFFER
 *   throttle_time => INT32
 *   results => error_code log_dir [topics] TAG_BUFFER
 *     error_code => INT16
 *     log_dir => COMPACT_STRING
 *     topics => name [partitions] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partitions => partition_index partition_size offset_lag is_future_key TAG_BUFFER
 *         partition_index => INT32
 *         partition_size => INT64
 *         offset_lag => INT64
 *         is_future_key => BOOLEAN
 */

const decodePartitions = decoder => {
  const partition = {
    partitionIndex: decoder.readInt32(),
    partitionSize: decoder.readInt64(),
    offsetLag: decoder.readInt64(),
    isFutureKey: decoder.readBoolean(),
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopics = decoder => {
  const topic = {
    name: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }

  decoder.readTaggedFields()
  return topic
}

const decodeResults = decoder => {
  const result = {
    errorCode: decoder.readInt16(),
    logDir: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(decodeTopics),
  }

  decoder.readTaggedFields()
  return result
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  return {
    throttleTime: decoder.readInt32(),
    results: decoder.readUVarIntArray(decodeResults),
  }
}

const parse = async data => {
  const resultsWithError = data.results.filter(({ errorCode }) => failure(errorCode))

  if (resultsWithError.length > 0) {
    throw createErrorFromCode(resultsWithError[0].errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
