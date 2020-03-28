/**
 * http://www.ietf.org/rfc/rfc5801.txt
 *
 * See org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse
 * for official Java client implementation.
 *
 * The mechanism consists of a message from the client to the server.
 * The client sends the "n,"" GS header, followed by the authorizationIdentitty
 * prefixed by "a=" (if present), followed by ",", followed by a US-ASCII SOH
 * character, followed by "auth=Bearer ", followed by the token value and then
 * closed by two additionals US-ASCII SOH characters.
 * The client may leave the authorization identity empty to
 * indicate that it is the same as the authentication identity.
 *
 * The server will verify the authentication token and verify that the
 * authentication credentials permit the client to login as the authorization
 * identity. If both steps succeed, the user is logged in.
 *
 * Kafkajs at the moment doesn't seem to mention SASL extension, but as the
 * Java implementation of OAUTHBEARER included it, it was also introduced here,
 * as it's just a matter of key-value concatenation.
 * Between the token and the closing SOH characters, SASL extension map can be
 * listed in OAuth "friendly" format: for each extension, concatenate the
 * extension entry key, followed by "=" followed by extension entry value
 * followed by a US-ASCII SOH character.
 */

const Encoder = require('../../encoder')

const SEPARATOR = '\u0001' // SOH - Start Of Header ASCII

module.exports = async ({ authorizationIdentity = null, extensions = {} }, oauthBearerToken) => {
  const authzid = authorizationIdentity == null ? '' : `"a=${authorizationIdentity}`
  let ext = ''
  for (const k in extensions) {
    ext += `${k}=${extensions[k]}${SEPARATOR}`
  }
  if (ext.length > 0) {
    ext = `${SEPARATOR}${ext}`
  }

  const oauthMsg = `n,${authzid},${SEPARATOR}auth=Bearer ${oauthBearerToken.value}${ext}${SEPARATOR}${SEPARATOR}`

  return {
    encode: async () => {
      return new Encoder().writeBytes(Buffer.from(oauthMsg))
    },
  }
}
