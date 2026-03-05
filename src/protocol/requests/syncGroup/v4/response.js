const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * SyncGroup Response (Version: 4) => throttle_time_ms error_code member_assignment TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   member_assignment => COMPACT_BYTES
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const memberAssignment = decoder.readUVarIntBytes()
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    memberAssignment,
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = {
  decode,
  parse,
}
