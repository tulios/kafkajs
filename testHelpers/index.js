const crypto = require('crypto')
const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')

module.exports = {
  secureRandom,
}
