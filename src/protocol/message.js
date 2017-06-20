const Encoder = require('./encoder')
const crc32 = require('./crc32')

/**
 * v0
 * Message => Crc MagicByte Attributes Key Value
 *   Crc => int32
 *   MagicByte => int8
 *   Attributes => int8
 *   Key => bytes
 *   Value => bytes
 *
 * v1 (supported since 0.10.0)
 * Message => Crc MagicByte Attributes Key Value
 *   Crc => int32
 *   MagicByte => int8
 *   Attributes => int8
 *   Timestamp => int64
 *   Key => bytes
 *   Value => bytes
 */

const Compression = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
}

module.exports = ({ magicByte = 0, attributes = Compression.None, key, value }) => {
  const content = new Encoder()
    .writeInt8(magicByte)
    .writeInt8(attributes)
    .writeBytes(key)
    .writeBytes(value)

  const crc = crc32(content)
  return new Encoder().writeInt32(crc).writeEncoder(content)
}
