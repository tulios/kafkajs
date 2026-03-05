const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * InitProducerId Response (Version: 2) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   producer_id => INT64
 *   producer_epoch => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const producerId = decoder.readInt64().toString()
  const producerEpoch = decoder.readInt16()
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    producerId,
    producerEpoch,
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
