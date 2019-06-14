const Long = require('long')

const INT8_SIZE = 1
const INT16_SIZE = 2
const INT32_SIZE = 4
const INT64_SIZE = 8

const MOST_SIGNIFICANT_BIT = 0x80 // 128
const OTHER_BITS = 0x7f // 127
const UNSIGNED_INT32_MAX_NUMBER = 0xffffff80
const UNSIGNED_INT64_MAX_NUMBER = Long.fromBytes([-1, -1, -1, -1, -1, -1, -1, -128])

module.exports = class Encoder {
  static encodeZigZag(value) {
    return (value << 1) ^ (value >> 31)
  }

  static encodeZigZag64(value) {
    const longValue = Long.fromValue(value)
    return longValue.shiftLeft(1).xor(longValue.shiftRight(63))
  }

  static sizeOfVarInt(value) {
    let encodedValue = this.encodeZigZag(value)
    let bytes = 1

    while ((encodedValue & UNSIGNED_INT32_MAX_NUMBER) !== 0) {
      bytes += 1
      encodedValue >>>= 7
    }

    return bytes
  }

  static sizeOfVarLong(value) {
    let longValue = Encoder.encodeZigZag64(value)
    let bytes = 1

    while (longValue.and(UNSIGNED_INT64_MAX_NUMBER).notEquals(Long.fromInt(0))) {
      bytes += 1
      longValue = longValue.shiftRightUnsigned(7)
    }

    return bytes
  }

  static sizeOfVarIntBytes(value) {
    const size = value == null ? -1 : Buffer.byteLength(value)

    if (size < 0) {
      return Encoder.sizeOfVarInt(-1)
    }

    return Encoder.sizeOfVarInt(size) + size
  }

  constructor() {
    this.buffer = Buffer.alloc(0)
  }

  writeInt8(value) {
    const tempBuffer = Buffer.alloc(INT8_SIZE)
    tempBuffer.writeInt8(value)
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeInt16(value) {
    const tempBuffer = Buffer.alloc(INT16_SIZE)
    tempBuffer.writeInt16BE(value)
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeInt32(value) {
    const tempBuffer = Buffer.alloc(INT32_SIZE)
    tempBuffer.writeInt32BE(value)
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeUInt32(value) {
    const tempBuffer = Buffer.alloc(INT32_SIZE)
    tempBuffer.writeUInt32BE(value)
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeInt64(value) {
    const tempBuffer = Buffer.alloc(INT64_SIZE)
    const longValue = Long.fromValue(value)
    tempBuffer.writeInt32BE(longValue.getHighBits(), 0)
    tempBuffer.writeInt32BE(longValue.getLowBits(), 4)
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeBoolean(value) {
    value ? this.writeInt8(1) : this.writeInt8(0)
    return this
  }

  writeString(value) {
    if (value == null) {
      this.writeInt16(-1)
      return this
    }

    const byteLength = Buffer.byteLength(value, 'utf8')
    this.writeInt16(byteLength)
    const tempBuffer = Buffer.alloc(byteLength)
    tempBuffer.write(value, 0, byteLength, 'utf8')
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeVarIntString(value) {
    if (value == null) {
      this.writeVarInt(-1)
      return this
    }

    const byteLength = Buffer.byteLength(value, 'utf8')
    this.writeVarInt(byteLength)
    const tempBuffer = Buffer.alloc(byteLength)
    tempBuffer.write(value, 0, byteLength, 'utf8')
    this.buffer = Buffer.concat([this.buffer, tempBuffer])
    return this
  }

  writeBytes(value) {
    if (value == null) {
      this.writeInt32(-1)
      return this
    }

    if (Buffer.isBuffer(value)) {
      // raw bytes
      this.writeInt32(value.length)
      this.buffer = Buffer.concat([this.buffer, value])
    } else {
      const valueToWrite = String(value)
      const byteLength = Buffer.byteLength(valueToWrite, 'utf8')
      this.writeInt32(byteLength)
      const tempBuffer = Buffer.alloc(byteLength)
      tempBuffer.write(valueToWrite, 0, byteLength, 'utf8')
      this.buffer = Buffer.concat([this.buffer, tempBuffer])
    }

    return this
  }

  writeVarIntBytes(value) {
    if (value == null) {
      this.writeVarInt(-1)
      return this
    }

    if (Buffer.isBuffer(value)) {
      // raw bytes
      this.writeVarInt(value.length)
      this.buffer = Buffer.concat([this.buffer, value])
    } else {
      const valueToWrite = String(value)
      const byteLength = Buffer.byteLength(valueToWrite, 'utf8')
      this.writeVarInt(byteLength)
      const tempBuffer = Buffer.alloc(byteLength)
      tempBuffer.write(valueToWrite, 0, byteLength, 'utf8')
      this.buffer = Buffer.concat([this.buffer, tempBuffer])
    }

    return this
  }

  writeEncoder(value) {
    if (value instanceof Encoder !== true) {
      throw new Error('value should be an instance of Encoder')
    }
    this.buffer = Buffer.concat([this.buffer, value.buffer])
    return this
  }

  writeEncoderArray(value) {
    if (!Array.isArray(value) || value.some(v => !(v instanceof Encoder))) {
      throw new Error('all values should be an instance of Encoder[]')
    }

    const newBuffer = [this.buffer]
    value.forEach(v => {
      newBuffer.push(v.buffer)
    })
    this.buffer = Buffer.concat(newBuffer)
    return this
  }

  writeBuffer(value) {
    if (value instanceof Buffer !== true) {
      throw new Error('value should be an instance of Buffer')
    }

    this.buffer = Buffer.concat([this.buffer, value])
    return this
  }

  writeNullableArray(array, type) {
    // A null value is encoded with length of -1 and there are no following bytes
    // On the context of this library, empty array and null are the same thing
    const length = array.length !== 0 ? array.length : -1
    return this.writeArray(array, type, length)
  }

  writeArray(array, type, length) {
    const arrayLength = length == null ? array.length : length
    this.writeInt32(arrayLength)
    if (type !== undefined) {
      switch (type) {
        case 'int32':
        case 'number':
          array.forEach(value => this.writeInt32(value))
          break
        case 'string':
          array.forEach(value => this.writeString(value))
          break
        case 'object':
          this.writeEncoderArray(array)
          break
      }
    } else {
      array.forEach(value => {
        switch (typeof value) {
          case 'int32':
          case 'number':
            this.writeInt32(value)
            break
          case 'string':
            this.writeString(value)
            break
          case 'object':
            this.writeEncoder(value)
            break
        }
      })
    }
    return this
  }

  writeVarIntArray(array, type) {
    if (type === 'object') {
      this.writeVarInt(array.length)
      this.writeEncoderArray(array)
    } else {
      const objectArray = array.filter(v => typeof v === 'object')
      this.writeVarInt(objectArray.length)
      this.writeEncoderArray(objectArray)
    }
    return this
  }

  // Based on:
  // https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/util/Varint.java#L106
  writeVarInt(value) {
    const byteArray = []
    let encodedValue = Encoder.encodeZigZag(value)

    while ((encodedValue & UNSIGNED_INT32_MAX_NUMBER) !== 0) {
      byteArray.push((encodedValue & OTHER_BITS) | MOST_SIGNIFICANT_BIT)
      encodedValue >>>= 7
    }

    byteArray.push(encodedValue & OTHER_BITS)
    this.buffer = Buffer.concat([this.buffer, Buffer.from(byteArray)])
    return this
  }

  writeVarLong(value) {
    const byteArray = []
    let longValue = Encoder.encodeZigZag64(value)

    while (longValue.and(UNSIGNED_INT64_MAX_NUMBER).notEquals(Long.fromInt(0))) {
      byteArray.push(
        longValue
          .and(OTHER_BITS)
          .or(MOST_SIGNIFICANT_BIT)
          .toInt()
      )
      longValue = longValue.shiftRightUnsigned(7)
    }

    byteArray.push(longValue.toInt())

    this.buffer = Buffer.concat([this.buffer, Buffer.from(byteArray)])
    return this
  }

  size() {
    return Buffer.byteLength(this.buffer)
  }

  toJSON() {
    return this.buffer.toJSON()
  }
}
