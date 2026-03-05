const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const { parse: parseV0 } = require('../v0/response')
const { failIfVersionNotSupported } = require('../../../error')

/**
 * SaslAuthenticate Response (Version: 2) => error_code error_message sasl_auth_bytes session_lifetime_ms TAG_BUFFER
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   sasl_auth_bytes => COMPACT_BYTES
 *   session_lifetime_ms => INT64
 */
const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)
  const errorMessage = decoder.readUVarIntString()

  // This is necessary to make the response compatible with the original
  // mechanism protocols. They expect a byte response, which starts with
  // the size
  const rawAuthBytes = decoder.readUVarIntBytes()
  const authBytesEncoder = new Encoder().writeBytes(rawAuthBytes)
  const authBytes = authBytesEncoder.buffer
  const sessionLifetimeMs = decoder.readInt64().toString()
  decoder.readTaggedFields()

  return {
    errorCode,
    errorMessage,
    authBytes,
    sessionLifetimeMs,
  }
}
module.exports = {
  decode,
  parse: parseV0,
}
