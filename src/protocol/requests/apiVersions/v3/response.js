const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')

/**
 * ApiVersions Response (Version: 3) => error_code [api_versions] throttle_time_ms TAG_BUFFER
 *   error_code => INT16
 *   api_versions => api_key min_version max_version TAG_BUFFER
 *     api_key => INT16
 *     min_version => INT16
 *     max_version => INT16
 *   throttle_time_ms => INT32
 */

const decodeApiVersion = decoder => {
  const apiVersion = {
    apiKey: decoder.readInt16(),
    minVersion: decoder.readInt16(),
    maxVersion: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return apiVersion
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const apiVersions = decoder.readUVarIntArray(decodeApiVersion)
  const throttleTime = decoder.readInt32()
  decoder.readTaggedFields()

  return {
    errorCode,
    apiVersions,
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
  }
}

const { parse } = require('../v0/response')

module.exports = {
  decode,
  parse,
}
