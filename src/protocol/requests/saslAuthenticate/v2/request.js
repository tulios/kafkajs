const Encoder = require('../../../encoder')
const { SaslAuthenticate: apiKey } = require('../../apiKeys')

/**
 * SaslAuthenticate Request (Version: 2) => sasl_auth_bytes TAG_BUFFER
 *   sasl_auth_bytes => COMPACT_BYTES
 */

module.exports = ({ authBytes }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'SaslAuthenticate',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntBytes(authBytes)
      .writeUVarIntBytes()
  },
})
