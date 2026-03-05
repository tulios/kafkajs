const Decoder = require('../../../decoder')
const { parse: parseV3 } = require('../v3/response')

/**
 * Produce Response (Version: 9) => [responses] throttle_time_ms TAG_BUFFER
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset [record_errors] error_message TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *       record_errors => batch_index batch_index_error_message TAG_BUFFER
 *         batch_index => INT32
 *         batch_index_error_message => COMPACT_NULLABLE_STRING
 *       error_message => COMPACT_NULLABLE_STRING
 *   throttle_time_ms => INT32
 */

const decodeRecordError = decoder => {
  const recordError = {
    batchIndex: decoder.readInt32(),
    batchIndexErrorMessage: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return recordError
}

const partition = decoder => {
  const partitionData = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    baseOffset: decoder.readInt64().toString(),
    logAppendTime: decoder.readInt64().toString(),
    logStartOffset: decoder.readInt64().toString(),
    recordErrors: decoder.readUVarIntArray(decodeRecordError),
    errorMessage: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return partitionData
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const topics = decoder.readUVarIntArray(decoder => {
    const topic = {
      topicName: decoder.readUVarIntString(),
      partitions: decoder.readUVarIntArray(partition),
    }
    decoder.readTaggedFields()
    return topic
  })

  const throttleTime = decoder.readInt32()
  decoder.readTaggedFields()

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
