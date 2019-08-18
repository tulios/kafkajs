const Long = require('long')
const { KafkaJSNonRetriableError } = require('../errors')

const DEFAULT_BUFFER_SIZE = 1024
const DEFAULT_ENCODING = 'utf8'

const INT8_SIZE = 1
const INT16_SIZE = 2
const INT32_SIZE = 4
const INT64_SIZE = 8

module.exports = class ExpandableBuffer {
  constructor({ size = 0 } = {}) {
    this._writeOffset = 0
    this._readOffset = 0
    this.length = 0

    if (size != null) {
      this._internalBuffer = Buffer.allocUnsafe(size)
    } else {
      this._internalBuffer = Buffer.allocUnsafe(DEFAULT_BUFFER_SIZE)
    }
  }

  toBuffer() {
    return this._internalBuffer.slice(0, this.length)
  }

  readInt8() {
    return this._readNumber(Buffer.prototype.readInt8, INT8_SIZE)
  }

  readInt16() {
    return this._readNumber(Buffer.prototype.readInt16BE, INT16_SIZE)
  }

  readInt32() {
    return this._readNumber(Buffer.prototype.readInt32BE, INT32_SIZE)
  }

  readInt64() {
    const highBits = this.buffer.readInt32()
    const lowBits = this.buffer.readInt32()

    return new Long(lowBits, highBits)
  }

  writeInt8(value) {
    this._writeNumber(Buffer.prototype.writeInt8, INT8_SIZE, value)
    return this
  }

  writeInt16(value) {
    this._writeNumber(Buffer.prototype.writeInt16BE, INT16_SIZE, value)
    return this
  }

  writeInt32(value) {
    this._writeNumber(Buffer.prototype.writeInt32BE, INT32_SIZE, value)
    return this
  }

  writeUInt32(value) {
    this._writeNumber(Buffer.prototype.writeUInt32BE, INT32_SIZE, value)
    return this
  }

  writeInt64(value) {
    const tempBuffer = Buffer.alloc(INT64_SIZE)
    const longValue = Long.fromValue(value)
    tempBuffer.writeInt32BE(longValue.getHighBits(), 0)
    tempBuffer.writeInt32BE(longValue.getLowBits(), INT32_SIZE)
    this.writeBuffer(tempBuffer)

    return this
  }

  writeBuffer(value) {
    this._writeBuffer(value)
    return this
  }

  writeString(value, encoding = DEFAULT_ENCODING) {
    const size = Buffer.byteLength(value, encoding)
    this._validateWrite(size)

    this._internalBuffer.write(value, this._writeOffset, size, encoding)
    this._writeOffset += size

    return this
  }

  _readNumber(fn, size) {
    this._validateRead(size)

    const value = fn.call(this._internalBuffer, this._readOffset)
    this._readOffset += size
    return value
  }

  _validateRead(size) {
    if (this._readOffset < 0 || this._readOffset + size > this.length) {
      throw new KafkaJSNonRetriableError('Attempted to read beyond buffer length')
    }
  }

  _writeNumber(fn, size, value) {
    this._validateWrite(size)

    fn.call(this._internalBuffer, value, this._writeOffset)
    this._writeOffset += size

    return this
  }

  _writeBuffer(value) {
    this._validateWrite(value.length)

    value.copy(this._internalBuffer, this._writeOffset)
    this._writeOffset += value.length

    return this
  }

  _validateWrite(size) {
    const currentSize = this._internalBuffer.length
    const minSize = this._writeOffset + size

    if (minSize > currentSize) {
      const data = this._internalBuffer
      const newSize = Math.max((currentSize * 3) / 2 + 1, minSize)

      this._internalBuffer = Buffer.allocUnsafe(newSize)
      data.copy(this._internalBuffer, 0, 0, currentSize)
    }

    if (this._writeOffset + minSize > this.length) {
      this.length = this._writeOffset + minSize
    }
  }
}
