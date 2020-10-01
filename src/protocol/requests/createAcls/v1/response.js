const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')

/**
 * CreateAcls Response (Version: 1) => throttle_time_ms [creation_responses]
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */

const decodeCreationResponse = decoder => ({
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const creationResponses = decoder.readArray(decodeCreationResponse)

  return {
    clientSideThrottleTime: throttleTime,
    creationResponses,
  }
}

module.exports = {
  decode,
  parse: parseV0,
}
