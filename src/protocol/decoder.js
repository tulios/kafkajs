const Long = require('long')

const INT8_SIZE = 1
const INT16_SIZE = 2
const INT32_SIZE = 4
const INT64_SIZE = 8

const MOST_SIGNIFICANT_BIT = 0x80 // 128
const OTHER_BITS = 0x7f // 127

module.exports = class Decoder {
  static int32Size() {
    return INT32_SIZE
  }

  static decodeZigZag(value) {
    return (value >>> 1) ^ -(value & 1)
  }

  static decodeZigZag64(longValue) {
    return longValue.shiftRightUnsigned(1).xor(longValue.and(Long.fromInt(1)).negate())
  }

  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  readInt8() {
    const value = this.buffer.readInt8(this.offset)
    this.offset += INT8_SIZE
    return value
  }

  readInt16() {
    const value = this.buffer.readInt16BE(this.offset)
    this.offset += INT16_SIZE
    return value
  }

  canReadInt32() {
    return Buffer.byteLength(this.buffer) - this.offset >= INT32_SIZE
  }

  readInt32() {
    const value = this.buffer.readInt32BE(this.offset)
    this.offset += INT32_SIZE
    return value
  }

  canReadInt64() {
    return Buffer.byteLength(this.buffer) - this.offset >= INT64_SIZE
  }

  readInt64() {
    const lowBits = this.buffer.readInt32BE(this.offset + 4)
    const highBits = this.buffer.readInt32BE(this.offset)
    const value = new Long(lowBits, highBits)
    this.offset += INT64_SIZE
    return value
  }

  readString() {
    const byteLength = this.readInt16()

    if (byteLength === -1) {
      return null
    }

    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    const value = stringBuffer.toString('utf8')
    this.offset += byteLength
    return value
  }

  readVarIntString() {
    const byteLength = this.readVarInt()

    if (byteLength === -1) {
      return null
    }

    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    const value = stringBuffer.toString('utf8')
    this.offset += byteLength
    return value
  }

  canReadBytes(length) {
    return Buffer.byteLength(this.buffer) - this.offset >= length
  }

  readBytes(byteLength = this.readInt32()) {
    if (byteLength === -1) {
      return null
    }

    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    this.offset += byteLength
    return stringBuffer
  }

  readVarIntBytes() {
    const byteLength = this.readVarInt()

    if (byteLength === -1) {
      return null
    }

    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    this.offset += byteLength
    return stringBuffer
  }

  readBoolean() {
    return this.readInt8() === 1
  }

  readAll() {
    const currentSize = Buffer.byteLength(this.buffer)
    const remainingBytes = currentSize - this.offset
    const newBuffer = Buffer.alloc(remainingBytes)
    this.buffer.copy(newBuffer, 0, this.offset, currentSize)
    return newBuffer
  }

  readArray(reader) {
    const length = this.readInt32()

    if (length === -1) {
      return []
    }

    const array = []
    for (let i = 0; i < length; i++) {
      array.push(reader(this))
    }

    return array
  }

  readVarIntArray(reader) {
    const length = this.readVarInt()

    if (length === -1) {
      return []
    }

    const array = []
    for (let i = 0; i < length; i++) {
      array.push(reader(this))
    }

    return array
  }

  async readArrayAsync(reader) {
    const length = this.readInt32()

    if (length === -1) {
      return []
    }

    const array = []
    for (let i = 0; i < length; i++) {
      array.push(await reader(this))
    }

    return array
  }

  readVarInt() {
    let currentByte
    let result = 0
    let i = 0

    do {
      currentByte = this.buffer[this.offset++]
      result += (currentByte & OTHER_BITS) << i
      i += 7
    } while (currentByte >= MOST_SIGNIFICANT_BIT)

    return Decoder.decodeZigZag(result)
  }

  readVarLong() {
    let currentByte
    let result = Long.fromInt(0)
    let i = 0

    do {
      currentByte = this.buffer[this.offset++]
      result = result.add(Long.fromInt(currentByte & OTHER_BITS).shiftLeft(i))
      i += 7
    } while (currentByte >= MOST_SIGNIFICANT_BIT)

    return Decoder.decodeZigZag64(result)
  }

  slice(size) {
    return new Decoder(this.buffer.slice(this.offset, this.offset + size))
  }

  forward(size) {
    this.offset += size
  }
}
