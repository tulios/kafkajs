const { KafkaJSNonRetriableError } = require('../../../errors')

if (typeof global !== 'undefined' && !global.crypto) {
  global.crypto = require('crypto')
}

const MAX_BYTES = 65536

module.exports = size => {
  if (size > MAX_BYTES) {
    throw new KafkaJSNonRetriableError(
      `Byte length (${size}) exceeds the max number of bytes of entropy available (${MAX_BYTES})`
    )
  }

  if (typeof global.crypto !== 'undefined' && global.crypto.randomBytes) {
    return global.crypto.randomBytes(size)
  }

  let cryptoImplementation
  if (typeof global.crypto !== 'undefined') {
    cryptoImplementation = global.crypto
  } else if (typeof global.msCrypto !== 'undefined') {
    cryptoImplementation = global.msCrypto
  }

  if (!cryptoImplementation) {
    throw new KafkaJSNonRetriableError('No available crypto implementation')
  }

  return cryptoImplementation.getRandomValues(Buffer.allocUnsafe(size))
}
