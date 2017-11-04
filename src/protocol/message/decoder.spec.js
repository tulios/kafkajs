const Encoder = require('../encoder')
const Decoder = require('../decoder')
const MessageDecoder = require('./decoder')

const MessageV0 = require('./v0')
const MessageV1 = require('./v1')

describe('Protocol > Message > decoder', () => {
  describe('v0', () => {
    test('decode', () => {
      const message = MessageV0({ key: 'v0-key', value: 'v0-value' })
      const offset = '0'
      const size = message.size()
      const { buffer } = new Encoder().writeInt32(size).writeEncoder(message)

      const decoder = new Decoder(buffer)
      decoder.readInt32() // read the size to be more realistic

      expect(MessageDecoder(offset, size, decoder)).toEqual({
        offset,
        size,
        crc: 1857563124,
        magicByte: 0,
        attributes: 0,
        key: Buffer.from('v0-key'),
        value: Buffer.from('v0-value'),
      })
    })

    test('throws an error if it is a partial message', () => {
      const message = MessageV0({ key: 'v0-key', value: 'v0-value' })
      const offset = '0'
      const size = message.size()
      const { buffer } = new Encoder().writeInt32(size).writeEncoder(message)

      const decoder = new Decoder(buffer)

      // read more to reduce the size of the buffer
      decoder.readInt32()
      decoder.readInt32()

      expect(() => MessageDecoder(offset, size, decoder)).toThrowError(
        /Tried to decode a partial message/
      )
    })
  })

  describe('v1', () => {
    test('decode', () => {
      const message = MessageV1({ key: 'v0-key', value: 'v0-value', timestamp: 1509827481681 })
      const offset = '0'
      const size = message.size()
      const { buffer } = new Encoder().writeInt32(size).writeEncoder(message)

      const decoder = new Decoder(buffer)
      decoder.readInt32() // read the size to be more realistic

      expect(MessageDecoder(offset, size, decoder)).toEqual({
        offset,
        size,
        crc: 1931824201,
        magicByte: 1,
        attributes: 0,
        timestamp: '1509827481681',
        key: Buffer.from('v0-key'),
        value: Buffer.from('v0-value'),
      })
    })
  })
})
