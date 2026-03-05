const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')

/**
 * DeleteGroups Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results => group_id error_code TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     error_code => INT16
 */

const decodeResult = decoder => {
  const result = {
    groupId: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return result
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const results = decoder.readUVarIntArray(decodeResult)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    results,
  }
}

module.exports = {
  decode,
  parse: parseV0,
}
