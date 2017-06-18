const crc32 = require('buffer-crc32')
const Encoder = require('../encoder')

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

const CODEC = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
}

module.exports = ({ offset, magicByte = 0, key, value }) => {
  const messageEncoder = new Encoder()
  messageEncoder.writeInt8(magicByte)
  messageEncoder.writeInt8(CODEC.None)
  messageEncoder.writeBytes(key)
  messageEncoder.writeBytes(value)

  const encoder = new Encoder()
  encoder.writeInt32(crc32(messageEncoder.buffer))
  encoder.writeEncoder(messageEncoder)
  return encoder
}
