const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ListGroups Response (Version: 4) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type group_state TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 *     group_state => COMPACT_STRING
 */

const decodeGroup = decoder => {
  const group = {
    groupId: decoder.readUVarIntString(),
    protocolType: decoder.readUVarIntString(),
    groupState: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return group
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const groups = decoder.readUVarIntArray(decodeGroup)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    groups,
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = {
  decodeGroup,
  decode,
  parse,
}
