const Encoder = require('../../encoder')

const US_ASCII_NULL_CHAR = '\u0000'

module.exports = sasl => ({
  encode: async () => {
    const props = sasl
    return new Encoder().writeBytes(
      [
        props.authorizationIdentity,
        props.accessKeyId,
        props.secretAccessKey,
        props.sessionToken,
      ].join(US_ASCII_NULL_CHAR)
    )
  },
})
