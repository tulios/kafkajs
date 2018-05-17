const Encoder = require('../../../encoder')
const Header = require('../../header/v0')

/**
 * v0
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   Key => varInt|Bytes
 *   Value => varInt|Bytes
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKey => VarInt|String
 *     HeaderValue => VarInt|Bytes
 */

module.exports = ({ timestampDelta, offsetDelta, key, value, headers = [] }) => {
  const sizeOfBody =
    1 + // always one byte for attributes
    Encoder.sizeOfVarInt(offsetDelta) +
    Encoder.sizeOfVarLong(timestampDelta) +
    Encoder.sizeOfVarIntBytes(key) +
    Encoder.sizeOfVarIntBytes(value) +
    sizeOfHeaders(headers)

  return new Encoder()
    .writeVarInt(sizeOfBody)
    .writeInt8(0) // no used record attributes at the moment
    .writeVarLong(timestampDelta)
    .writeVarInt(offsetDelta)
    .writeVarIntBytes(key)
    .writeVarIntBytes(value)
    .writeVarIntArray(headers.map(Header))
}

const sizeOfHeaders = headers => {
  let size = Encoder.sizeOfVarInt(headers.length)

  for (let header of headers) {
    const keySize = Buffer.byteLength(header.key)
    const valueSize = Buffer.byteLength(header.value)

    size += Encoder.sizeOfVarInt(keySize) + keySize

    if (header.value === null) {
      size += Encoder.sizeOfVarInt(-1)
    } else {
      size += Encoder.sizeOfVarInt(valueSize) + valueSize
    }
  }

  return size
}
