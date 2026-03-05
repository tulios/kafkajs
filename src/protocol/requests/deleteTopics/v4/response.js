const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')

/**
 * DeleteTopics Response (Version: 4) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => name error_code TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 */

const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

const topicErrors = decoder => {
  const result = {
    topic: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
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
  parse: parseV0,
}
