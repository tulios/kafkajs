const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * IncrementalAlterConfigs Response (Version: 1) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 */

const decodeResponse = decoder => {
  const response = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
  return response
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeResponse)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
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
