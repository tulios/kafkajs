const crypto = require('crypto')
const scram = require('../../protocol/sasl/scram')

const EQUAL_SIGN_REGEX = /=/g
const COMMA_SIGN_REGEX = /,/g

const URLSAFE_BASE64_PLUS_REGEX = /\+/g
const URLSAFE_BASE64_SLASH_REGEX = /\//g
const URLSAFE_BASE64_TRAILING_EQUAL_REGEX = /=+$/

module.exports = class SCRAM {
  /**
   * From https://tools.ietf.org/html/rfc5802#section-5.1
   *
   * The characters ',' or '=' in usernames are sent as '=2C' and
   * '=3D' respectively.  If the server receives a username that
   * contains '=' not followed by either '2C' or '3D', then the
   * server MUST fail the authentication.
   */
  static sanitizeString(str) {
    return str.replace(EQUAL_SIGN_REGEX, '=3D').replace(COMMA_SIGN_REGEX, '=2C')
  }

  /**
   * In cryptography, a nonce is an arbitrary number that can be used just once.
   * It is similar in spirit to a nonce * word, hence the name. It is often a random or pseudo-random
   * number issued in an authentication protocol to * ensure that old communications cannot be reused
   * in replay attacks.
   */
  static nonce() {
    return crypto
      .randomBytes(16)
      .toString('base64')
      .replace(URLSAFE_BASE64_PLUS_REGEX, '-') // make it url safe
      .replace(URLSAFE_BASE64_SLASH_REGEX, '_')
      .replace(URLSAFE_BASE64_TRAILING_EQUAL_REGEX, '')
      .toString('ascii')
  }

  /**
   * @param {Connection} connection
   */
  constructor(connection) {
    this.connection = connection
    this.currentNonce = SCRAM.nonce()
  }

  async sendFirstClientMessage() {
    const gs2Header = 'n,,'
    const clientFirstMessage = `${gs2Header}${this.firstMessageBare()}`
    const request = scram.firstMessage.request({ clientFirstMessage })
    const response = scram.firstMessage.response

    return this.connection.authenticate({
      authExpectResponse: true,
      request,
      response,
    })
  }

  firstMessageBare() {
    return `n=${this.encodedUsername()},r=${this.currentNonce}`
  }

  encodedUsername() {
    const { username } = this.connection.sasl
    return SCRAM.sanitizeString(username).toString('utf-8')
  }
}
