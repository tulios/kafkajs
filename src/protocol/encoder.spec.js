const Encoder = require('./encoder')
const Decoder = require('./decoder')

const MAX_SAFE_POSITIVE_SIGNED_INT = 2147483647
const MIN_SAFE_NEGATIVE_SIGNED_INT = -2147483648

describe('Protocol > Encoder', () => {
  const unsigned32 = number => new Encoder().writeUnsignedVarInt32(number).buffer
  const signed32 = number => new Encoder().writeSignedVarInt32(number).buffer
  const decodeUnsigned32 = buffer => new Decoder(buffer).readUnsignedVarInt32()
  const decodeSigned32 = buffer => new Decoder(buffer).readSignedVarInt32()

  describe('varint', () => {
    test('encode signed int32 numbers', () => {
      expect(signed32(0)).toEqual(Buffer.from([0x00]))
      expect(signed32(1)).toEqual(Buffer.from([0x02]))
      expect(signed32(63)).toEqual(Buffer.from([0x7e]))
      expect(signed32(64)).toEqual(Buffer.from([0x80, 0x01]))
      expect(signed32(8191)).toEqual(Buffer.from([0xfe, 0x7f]))
      expect(signed32(8192)).toEqual(Buffer.from([0x80, 0x80, 0x01]))
      expect(signed32(1048575)).toEqual(Buffer.from([0xfe, 0xff, 0x7f]))
      expect(signed32(1048576)).toEqual(Buffer.from([0x80, 0x80, 0x80, 0x01]))
      expect(signed32(134217727)).toEqual(Buffer.from([0xfe, 0xff, 0xff, 0x7f]))
      expect(signed32(134217728)).toEqual(Buffer.from([0x80, 0x80, 0x80, 0x80, 0x01]))

      expect(signed32(-1)).toEqual(Buffer.from([0x01]))
      expect(signed32(-64)).toEqual(Buffer.from([0x7f]))
      expect(signed32(-65)).toEqual(Buffer.from([0x81, 0x01]))
      expect(signed32(-8192)).toEqual(Buffer.from([0xff, 0x7f]))
      expect(signed32(-8193)).toEqual(Buffer.from([0x81, 0x80, 0x01]))
      expect(signed32(-1048576)).toEqual(Buffer.from([0xff, 0xff, 0x7f]))
      expect(signed32(-1048577)).toEqual(Buffer.from([0x81, 0x80, 0x80, 0x01]))
      expect(signed32(-134217728)).toEqual(Buffer.from([0xff, 0xff, 0xff, 0x7f]))
      expect(signed32(-134217729)).toEqual(Buffer.from([0x81, 0x80, 0x80, 0x80, 0x01]))
    })

    test('encode signed int32 boundaries', () => {
      expect(signed32(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(
        Buffer.from([0xfe, 0xff, 0xff, 0xff, 0x0f])
      )
      expect(signed32(MIN_SAFE_NEGATIVE_SIGNED_INT)).toEqual(
        Buffer.from([0xff, 0xff, 0xff, 0xff, 0x0f])
      )
    })

    test('decode int32 numbers', () => {
      expect(decodeSigned32(signed32(0))).toEqual(0)
      expect(decodeSigned32(signed32(1))).toEqual(1)
      expect(decodeSigned32(signed32(63))).toEqual(63)
      expect(decodeSigned32(signed32(64))).toEqual(64)
      expect(decodeSigned32(signed32(8191))).toEqual(8191)
      expect(decodeSigned32(signed32(8192))).toEqual(8192)
      expect(decodeSigned32(signed32(1048575))).toEqual(1048575)
      expect(decodeSigned32(signed32(1048576))).toEqual(1048576)
      expect(decodeSigned32(signed32(134217727))).toEqual(134217727)
      expect(decodeSigned32(signed32(134217728))).toEqual(134217728)

      expect(decodeSigned32(signed32(-1))).toEqual(-1)
      expect(decodeSigned32(signed32(-64))).toEqual(-64)
      expect(decodeSigned32(signed32(-65))).toEqual(-65)
      expect(decodeSigned32(signed32(-8192))).toEqual(-8192)
      expect(decodeSigned32(signed32(-8193))).toEqual(-8193)
      expect(decodeSigned32(signed32(-1048576))).toEqual(-1048576)
      expect(decodeSigned32(signed32(-1048577))).toEqual(-1048577)
      expect(decodeSigned32(signed32(-134217728))).toEqual(-134217728)
      expect(decodeSigned32(signed32(-134217729))).toEqual(-134217729)
    })

    test('decode signed int32 boundaries', () => {
      expect(decodeSigned32(signed32(MAX_SAFE_POSITIVE_SIGNED_INT))).toEqual(
        MAX_SAFE_POSITIVE_SIGNED_INT
      )
      expect(decodeSigned32(signed32(MIN_SAFE_NEGATIVE_SIGNED_INT))).toEqual(
        MIN_SAFE_NEGATIVE_SIGNED_INT
      )
    })
  })
})
