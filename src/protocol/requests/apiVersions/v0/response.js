const Decoder = require('../../../decoder')

/**
 * ApiVersionResponse => ApiVersions
 *   ErrorCode = INT16
 *   ApiVersions = [ApiVersion]
 *     ApiVersion = ApiKey MinVersion MaxVersion
 *       ApiKey = INT16
 *       MinVersion = INT16
 *       MaxVersion = INT16
 */

const apiVersion = decoder => ({
  apiKey: decoder.readInt16(),
  minVersion: decoder.readInt16(),
  maxVersion: decoder.readInt16(),
})

module.exports = data => {
  const decoder = new Decoder(data)
  return {
    errorCode: decoder.readInt16(),
    apiVersions: decoder.readArray(apiVersion),
  }
}
