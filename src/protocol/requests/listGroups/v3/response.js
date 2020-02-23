const responseV2 = require('../v2/response')
const Decoder = require('../../../decoder')

/**
 * ListGroups Response (Version: 3) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 */

const decodeGroup = decoder => ({
  groupId: decoder.readVarIntString(),
  protocolType: decoder.readVarIntString(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const groups = decoder.readArray(decodeGroup)

  return {
    throttleTime,
    errorCode,
    groups,
  }
}

module.exports = {
  decode,
  parse: responseV2.parse,
}
