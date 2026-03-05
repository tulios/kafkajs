const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * Heartbeat Response (Version: 4) => throttle_time_ms error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  decoder.readTaggedFields()

  failIfVersionNotSupported(errorCode)

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = { decode, parse }
