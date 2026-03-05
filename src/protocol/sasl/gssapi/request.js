/**
 * GSSAPI (Kerberos) SASL Authentication
 *
 * The GSSAPI mechanism uses Kerberos tickets for authentication.
 * The client sends a Kerberos AP-REQ token to the broker, and
 * the broker responds with an AP-REP token.
 *
 * This implementation wraps the GSSAPI token exchange using the
 * `kerberos` npm package for native Kerberos support.
 *
 * @see https://tools.ietf.org/html/rfc4752
 * @see https://kafka.apache.org/documentation/#security_sasl_kerberos
 */

const Encoder = require('../../encoder')

module.exports = ({ token }) => ({
  encode: async () => {
    return new Encoder().writeBytes(Buffer.from(token, 'base64')).buffer
  },
})
