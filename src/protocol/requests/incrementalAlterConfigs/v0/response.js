const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 */

const decodeResponse = decoder => ({
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
  resourceType: decoder.readInt8(),
  resourceName: decoder.readString(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = decoder.readArray(decodeResponse)

  return {
    throttleTime,
    responses,
  }
}

const parse = async data => {
  const responsesWithError = data.responses.filter(({ errorCode }) => failure(errorCode))
  if (responsesWithError.length > 0) {
    throw createErrorFromCode(responsesWithError[0].errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
