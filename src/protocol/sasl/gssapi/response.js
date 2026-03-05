const Decoder = require('../../decoder')

module.exports = {
  decode: async rawData => {
    const decoder = new Decoder(rawData)
    return decoder.readBytes()
  },
  parse: async data => {
    return data.toString('base64')
  },
}
