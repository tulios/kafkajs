const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * DescribeLogDirs Response (Version: 0) => throttle_time [results]
 *   throttle_time => INT32
 *   results => error_code log_dir [topics]
 *     error_code => INT16
 *     log_dir => STRING
 *     topics => name [partitions]
 *       name => STRING
 *       partitions => partition_index partition_size offset_lag is_future_key
 *         partition_index => INT32
 *         partition_size => INT64
 *         offset_lag => INT64
 *         is_future_key => BOOLEAN
 */

const decodePartitions = decoder => ({
  partitionIndex: decoder.readInt32(),
  partitionSize: decoder.readInt64(),
  offsetLag: decoder.readInt64(),
  isFutureKey: decoder.readBoolean(),
})

const decodeTopics = decoder => ({
  name: decoder.readString(),
  partitions: decoder.readArray(decodePartitions),
})

const decodeResults = decoder => ({
  errorCode: decoder.readInt16(),
  logDir: decoder.readString(),
  topics: decoder.readArray(decodeTopics),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    throttleTime: decoder.readInt32(),
    results: decoder.readArray(decodeResults),
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
