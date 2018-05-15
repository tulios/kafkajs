const Encoder = require('./encoder')
const Decoder = require('./decoder')

describe('Protocol > Encoder', () => {
  const unsigned32 = number => new Encoder().writeUnsignedVarInt32(number).buffer
  const signed32 = number => new Encoder().writeSignedVarInt32(number).buffer
  const decodeUnsigned32 = buffer => new Decoder(buffer).readUnsignedVarInt32()
  const decodeSigned32 = buffer => new Decoder(buffer).readSignedVarInt32()

  describe('varint', () => {
    test('encode numbers', () => {
      expect(decodeUnsigned32(unsigned32(0))).toEqual(0)
      expect(decodeUnsigned32(unsigned32(4))).toEqual(4)
      expect(decodeUnsigned32(unsigned32(300))).toEqual(300)

      expect(decodeSigned32(signed32(0))).toEqual(0)
      expect(decodeSigned32(signed32(4))).toEqual(4)
      expect(decodeSigned32(signed32(-2))).toEqual(-2)
    })
  })
})
