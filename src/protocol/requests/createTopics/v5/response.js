const Decoder = require('../../../decoder')
const { parse: parseV1 } = require('../v1/response')

/**
 * CreateTopics Response (Version: 5) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name error_code error_message TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 */

const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

const topicErrors = decoder => {
  const result = {
    topic: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return result
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const topics = decoder.readUVarIntArray(topicErrors).sort(topicNameComparator)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    topicErrors: topics,
  }
}

module.exports = {
  decode,
  parse: parseV1,
}
