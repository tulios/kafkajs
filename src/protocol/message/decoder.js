const { KafkaJSPartialMessageError, KafkaJSError } = require('../../errors')
const V0Decoder = require('./v0/decoder')

const decodeMessage = (decoder, magicByte) => {
  switch (magicByte) {
    case 0:
      return V0Decoder(decoder)
    default:
      throw new KafkaJSError(`Unsupported message version, magic byte: ${magicByte}`)
  }
}

module.exports = (offset, size, decoder) => {
  const remainingBytes = decoder.buffer.length - decoder.offset

  if (remainingBytes < size) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial message: remainingBytes(${remainingBytes}) < messageSize(${size})`
    )
  }

  const crc = decoder.readInt32()
  const magicByte = decoder.readInt8()
  const message = decodeMessage(decoder, magicByte)
  return Object.assign({ offset, size, crc, magicByte }, message)
}
