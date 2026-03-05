const Decoder = require('../../../decoder')
const { parse: parseV3 } = require('../v3/response')

/**
 * Produce Response (Version: 8) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset [record_errors] error_message
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *       record_errors => batch_index batch_index_error_message
 *         batch_index => INT32
 *         batch_index_error_message => NULLABLE_STRING
 *       error_message => NULLABLE_STRING
 *   throttle_time_ms => INT32
 */

const decodeRecordError = decoder => ({
  batchIndex: decoder.readInt32(),
  batchIndexErrorMessage: decoder.readString(),
})

const partition = decoder => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  baseOffset: decoder.readInt64().toString(),
  logAppendTime: decoder.readInt64().toString(),
  logStartOffset: decoder.readInt64().toString(),
  recordErrors: decoder.readArray(decodeRecordError),
  errorMessage: decoder.readString(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const topics = decoder.readArray(decoder => ({
    topicName: decoder.readString(),
    partitions: decoder.readArray(partition),
  }))

  const throttleTime = decoder.readInt32()

  return {
    topics,
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
  }
}

module.exports = {
  decode,
  parse: parseV3,
}
