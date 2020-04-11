const responseV0 = require('../v0/response')
const Decoder = require('../../../decoder')

/**
 * CreateAcls Response (Version: 1) => throttle_time_ms [creation_responses]
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const creationResponses = decoder.readArray(responseV0.decodeCreationResponse)

  return {
    throttleTime,
    creationResponses,
  }
}

module.exports = {
  decode,
  parse: responseV0.parse,
}
