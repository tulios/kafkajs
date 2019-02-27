const crypto = global.crypto
  ? {
      randomBytes: size => {
        let bytes = Buffer.allocUnsafe(size)
        global.crypto.getRandomValues(bytes)
        return bytes
      },
    }
  : require('crypto')

module.exports = size => crypto.randomBytes(size)
