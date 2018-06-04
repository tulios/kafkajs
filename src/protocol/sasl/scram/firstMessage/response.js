const Decoder = require('../../../decoder')

module.exports = {
  decode: async rawData => {
    return new Decoder(rawData).readBytes()
  },
  parse: async data => {
    return data
      .toString()
      .split(',')
      .map(str => str.split('='))
      .reduce((obj, entry) => ({ ...obj, [entry[0]]: entry[1] }), {})
  },
}
