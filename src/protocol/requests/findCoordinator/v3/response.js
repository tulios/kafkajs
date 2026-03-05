const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * FindCoordinator Response (Version: 3) => throttle_time_ms error_code error_message coordinator TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   coordinator => node_id host port TAG_BUFFER
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const errorMessage = decoder.readUVarIntString()
  const coordinator = {
    nodeId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
  }
  decoder.readTaggedFields()
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    errorMessage,
    coordinator,
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
