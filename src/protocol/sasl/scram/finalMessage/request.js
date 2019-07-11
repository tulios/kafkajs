const Encoder = require('../../../encoder')

module.exports = sasl => ({
  encode: async () => {
    const props = typeof sasl === 'function' ? sasl() : sasl
    new Encoder().writeBytes(props.finalMessage)
  },
})
