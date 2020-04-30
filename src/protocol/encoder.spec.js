const Long = require('long')

const Encoder = require('./encoder')
const Decoder = require('./decoder')

const MAX_SAFE_POSITIVE_SIGNED_INT = 2147483647
const MIN_SAFE_NEGATIVE_SIGNED_INT = -2147483648

describe('Protocol > Encoder', () => {
  const signed32 = number => new Encoder().writeVarInt(number).buffer
  const decode32 = buffer => new Decoder(buffer).readVarInt()

  const signed64 = number => new Encoder().writeVarLong(number).buffer
  const decode64 = buffer => new Decoder(buffer).readVarLong()

  const B = (...args) => Buffer.from(args)
  const L = value => Long.fromString(`${value}`)

  describe('writeEncoder', () => {
    it('should throw if the value is not an Encoder', () => {
      const encoder = new Encoder()
      expect(() => encoder.writeEncoder()).toThrow('value should be an instance of Encoder')
    })

    it('should append the value buffer to the existing encoder', () => {
      const encoder = new Encoder().writeBuffer(B(1)).writeEncoder(new Encoder().writeBuffer(B(2)))
      expect(encoder.buffer).toEqual(B(1, 2))
    })
  })

  describe('writeEncoderArray', () => {
    it('should throw if any of the elements in the array are not encoders', () => {
      const values = [new Encoder(), 'not an encoder']
      expect(() => new Encoder().writeEncoderArray(values)).toThrow(
        'all values should be an instance of Encoder[]'
      )
    })

    it('should append all encoder values to the existing encoder', () => {
      const values = [
        new Encoder().writeBuffer(B(1)),
        new Encoder().writeBuffer(B(2)),
        new Encoder().writeBuffer(B(3)),
      ]

      expect(new Encoder().writeEncoderArray(values).buffer).toEqual(B(1, 2, 3))
    })
  })

  describe('varint', () => {
    test('encode signed int32 numbers', () => {
      expect(signed32(0)).toEqual(B(0x00))
      expect(signed32(1)).toEqual(B(0x02))
      expect(signed32(63)).toEqual(B(0x7e))
      expect(signed32(64)).toEqual(B(0x80, 0x01))
      expect(signed32(8191)).toEqual(B(0xfe, 0x7f))
      expect(signed32(8192)).toEqual(B(0x80, 0x80, 0x01))
      expect(signed32(1048575)).toEqual(B(0xfe, 0xff, 0x7f))
      expect(signed32(1048576)).toEqual(B(0x80, 0x80, 0x80, 0x01))
      expect(signed32(134217727)).toEqual(B(0xfe, 0xff, 0xff, 0x7f))
      expect(signed32(134217728)).toEqual(B(0x80, 0x80, 0x80, 0x80, 0x01))

      expect(signed32(-1)).toEqual(B(0x01))
      expect(signed32(-64)).toEqual(B(0x7f))
      expect(signed32(-65)).toEqual(B(0x81, 0x01))
      expect(signed32(-8192)).toEqual(B(0xff, 0x7f))
      expect(signed32(-8193)).toEqual(B(0x81, 0x80, 0x01))
      expect(signed32(-1048576)).toEqual(B(0xff, 0xff, 0x7f))
      expect(signed32(-1048577)).toEqual(B(0x81, 0x80, 0x80, 0x01))
      expect(signed32(-134217728)).toEqual(B(0xff, 0xff, 0xff, 0x7f))
      expect(signed32(-134217729)).toEqual(B(0x81, 0x80, 0x80, 0x80, 0x01))
    })

    test('encode signed int32 boundaries', () => {
      expect(signed32(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0x0f))
      expect(signed32(MIN_SAFE_NEGATIVE_SIGNED_INT)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x0f))
    })

    test('decode int32 numbers', () => {
      expect(decode32(signed32(0))).toEqual(0)
      expect(decode32(signed32(1))).toEqual(1)
      expect(decode32(signed32(63))).toEqual(63)
      expect(decode32(signed32(64))).toEqual(64)
      expect(decode32(signed32(8191))).toEqual(8191)
      expect(decode32(signed32(8192))).toEqual(8192)
      expect(decode32(signed32(1048575))).toEqual(1048575)
      expect(decode32(signed32(1048576))).toEqual(1048576)
      expect(decode32(signed32(134217727))).toEqual(134217727)
      expect(decode32(signed32(134217728))).toEqual(134217728)

      expect(decode32(signed32(-1))).toEqual(-1)
      expect(decode32(signed32(-64))).toEqual(-64)
      expect(decode32(signed32(-65))).toEqual(-65)
      expect(decode32(signed32(-8192))).toEqual(-8192)
      expect(decode32(signed32(-8193))).toEqual(-8193)
      expect(decode32(signed32(-1048576))).toEqual(-1048576)
      expect(decode32(signed32(-1048577))).toEqual(-1048577)
      expect(decode32(signed32(-134217728))).toEqual(-134217728)
      expect(decode32(signed32(-134217729))).toEqual(-134217729)
    })

    test('decode signed int32 boundaries', () => {
      expect(decode32(signed32(MAX_SAFE_POSITIVE_SIGNED_INT))).toEqual(MAX_SAFE_POSITIVE_SIGNED_INT)
      expect(decode32(signed32(MIN_SAFE_NEGATIVE_SIGNED_INT))).toEqual(MIN_SAFE_NEGATIVE_SIGNED_INT)
    })
  })

  describe('varlong', () => {
    test('encode signed int64 number', () => {
      expect(signed64(0)).toEqual(B(0x00))
      expect(signed64(1)).toEqual(B(0x02))
      expect(signed64(63)).toEqual(B(0x7e))
      expect(signed64(64)).toEqual(B(0x80, 0x01))
      expect(signed64(8191)).toEqual(B(0xfe, 0x7f))
      expect(signed64(8192)).toEqual(B(0x80, 0x80, 0x01))
      expect(signed64(1048575)).toEqual(B(0xfe, 0xff, 0x7f))
      expect(signed64(1048576)).toEqual(B(0x80, 0x80, 0x80, 0x01))
      expect(signed64(134217727)).toEqual(B(0xfe, 0xff, 0xff, 0x7f))
      expect(signed64(134217728)).toEqual(B(0x80, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0x0f))
      expect(signed64(L('17179869183'))).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('17179869184'))).toEqual(B(0x80, 0x80, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(L('2199023255551'))).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('2199023255552'))).toEqual(B(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(L('281474976710655'))).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('281474976710656'))).toEqual(
        B(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
      )
      expect(signed64(L('36028797018963967'))).toEqual(
        B(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)
      )
      expect(signed64(L('36028797018963968'))).toEqual(
        B(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
      )
      expect(signed64(L('4611686018427387903'))).toEqual(
        B(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)
      )
      expect(signed64(L('4611686018427387904'))).toEqual(
        B(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
      )
      expect(signed64(Long.MAX_VALUE)).toEqual(
        B(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
      )

      expect(signed64(-1)).toEqual(B(0x01))
      expect(signed64(-64)).toEqual(B(0x7f))
      expect(signed64(-65)).toEqual(B(0x81, 0x01))
      expect(signed64(-8192)).toEqual(B(0xff, 0x7f))
      expect(signed64(-8193)).toEqual(B(0x81, 0x80, 0x01))
      expect(signed64(-1048576)).toEqual(B(0xff, 0xff, 0x7f))
      expect(signed64(-1048577)).toEqual(B(0x81, 0x80, 0x80, 0x01))
      expect(signed64(-134217728)).toEqual(B(0xff, 0xff, 0xff, 0x7f))
      expect(signed64(-134217729)).toEqual(B(0x81, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(MIN_SAFE_NEGATIVE_SIGNED_INT)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x0f))
      expect(signed64(L('-17179869184'))).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('-17179869185'))).toEqual(B(0x81, 0x80, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(L('-2199023255552'))).toEqual(B(0xff, 0xff, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('-2199023255553'))).toEqual(B(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01))
      expect(signed64(L('-281474976710656'))).toEqual(B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f))
      expect(signed64(L('-281474976710657'))).toEqual(
        B(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 1)
      )
      expect(signed64(L('-36028797018963968'))).toEqual(
        B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)
      )
      expect(signed64(L('-36028797018963969'))).toEqual(
        B(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
      )
      expect(signed64(L('-4611686018427387904'))).toEqual(
        B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)
      )
      expect(signed64(L('-4611686018427387905'))).toEqual(
        B(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
      )
      expect(signed64(Long.MIN_VALUE)).toEqual(
        B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
      )
    })

    test('decode signed int64 number', () => {
      expect(decode64(signed64(0))).toEqual(L(0))
      expect(decode64(signed64(1))).toEqual(L(1))
      expect(decode64(signed64(63))).toEqual(L(63))
      expect(decode64(signed64(64))).toEqual(L(64))
      expect(decode64(signed64(8191))).toEqual(L(8191))
      expect(decode64(signed64(8192))).toEqual(L(8192))
      expect(decode64(signed64(1048575))).toEqual(L(1048575))
      expect(decode64(signed64(1048576))).toEqual(L(1048576))
      expect(decode64(signed64(134217727))).toEqual(L(134217727))
      expect(decode64(signed64(134217728))).toEqual(L(134217728))
      expect(decode64(signed64(MAX_SAFE_POSITIVE_SIGNED_INT))).toEqual(
        L(MAX_SAFE_POSITIVE_SIGNED_INT)
      )
      expect(decode64(signed64(L('17179869183')))).toEqual(L('17179869183'))
      expect(decode64(signed64(L('17179869184')))).toEqual(L('17179869184'))
      expect(decode64(signed64(L('2199023255551')))).toEqual(L('2199023255551'))
      expect(decode64(signed64(L('2199023255552')))).toEqual(L('2199023255552'))
      expect(decode64(signed64(L('281474976710655')))).toEqual(L('281474976710655'))
      expect(decode64(signed64(L('281474976710656')))).toEqual(L('281474976710656'))
      expect(decode64(signed64(L('36028797018963967')))).toEqual(L('36028797018963967'))
      expect(decode64(signed64(L('36028797018963968')))).toEqual(L('36028797018963968'))
      expect(decode64(signed64(L('4611686018427387903')))).toEqual(L('4611686018427387903'))
      expect(decode64(signed64(L('4611686018427387904')))).toEqual(L('4611686018427387904'))
      expect(decode64(signed64(Long.MAX_VALUE))).toEqual(Long.MAX_VALUE)
    })
  })

  describe('sizeOfVarInt', () => {
    it('returns the size in bytes', () => {
      expect(Encoder.sizeOfVarInt(0)).toEqual(signed32(1).length)
      expect(Encoder.sizeOfVarInt(1)).toEqual(signed32(1).length)
      expect(Encoder.sizeOfVarInt(63)).toEqual(signed32(63).length)
      expect(Encoder.sizeOfVarInt(64)).toEqual(signed32(64).length)
      expect(Encoder.sizeOfVarInt(8191)).toEqual(signed32(8191).length)
      expect(Encoder.sizeOfVarInt(8192)).toEqual(signed32(8192).length)
      expect(Encoder.sizeOfVarInt(1048575)).toEqual(signed32(1048575).length)
      expect(Encoder.sizeOfVarInt(1048576)).toEqual(signed32(1048576).length)
      expect(Encoder.sizeOfVarInt(134217727)).toEqual(signed32(134217727).length)
      expect(Encoder.sizeOfVarInt(134217728)).toEqual(signed32(134217728).length)

      expect(Encoder.sizeOfVarInt(-1)).toEqual(signed32(-1).length)
      expect(Encoder.sizeOfVarInt(-64)).toEqual(signed32(-64).length)
      expect(Encoder.sizeOfVarInt(-65)).toEqual(signed32(-65).length)
      expect(Encoder.sizeOfVarInt(-8192)).toEqual(signed32(-8192).length)
      expect(Encoder.sizeOfVarInt(-8193)).toEqual(signed32(-8193).length)
      expect(Encoder.sizeOfVarInt(-1048576)).toEqual(signed32(-1048576).length)
      expect(Encoder.sizeOfVarInt(-1048577)).toEqual(signed32(-1048577).length)
      expect(Encoder.sizeOfVarInt(-134217728)).toEqual(signed32(-134217728).length)
      expect(Encoder.sizeOfVarInt(-134217729)).toEqual(signed32(-134217729).length)

      expect(Encoder.sizeOfVarInt(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(
        signed32(MAX_SAFE_POSITIVE_SIGNED_INT).length
      )
      expect(Encoder.sizeOfVarInt(MIN_SAFE_NEGATIVE_SIGNED_INT)).toEqual(
        signed32(MIN_SAFE_NEGATIVE_SIGNED_INT).length
      )
    })
  })

  describe('sizeOfVarLong', () => {
    it('returns the size in bytes', () => {
      expect(Encoder.sizeOfVarLong(0)).toEqual(signed64(0).length)
      expect(Encoder.sizeOfVarLong(1)).toEqual(signed64(1).length)
      expect(Encoder.sizeOfVarLong(63)).toEqual(signed64(63).length)
      expect(Encoder.sizeOfVarLong(64)).toEqual(signed64(64).length)
      expect(Encoder.sizeOfVarLong(8191)).toEqual(signed64(8191).length)
      expect(Encoder.sizeOfVarLong(8192)).toEqual(signed64(8192).length)
      expect(Encoder.sizeOfVarLong(1048575)).toEqual(signed64(1048575).length)
      expect(Encoder.sizeOfVarLong(1048576)).toEqual(signed64(1048576).length)
      expect(Encoder.sizeOfVarLong(134217727)).toEqual(signed64(134217727).length)
      expect(Encoder.sizeOfVarLong(134217728)).toEqual(signed64(134217728).length)

      expect(Encoder.sizeOfVarLong(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(
        signed64(MAX_SAFE_POSITIVE_SIGNED_INT).length
      )

      expect(Encoder.sizeOfVarLong(L('17179869183'))).toEqual(signed64(L('17179869183')).length)
      expect(Encoder.sizeOfVarLong(L('17179869184'))).toEqual(signed64(L('17179869184')).length)
      expect(Encoder.sizeOfVarLong(L('2199023255551'))).toEqual(signed64(L('2199023255551')).length)
      expect(Encoder.sizeOfVarLong(L('2199023255552'))).toEqual(signed64(L('2199023255552')).length)
      expect(Encoder.sizeOfVarLong(L('281474976710655'))).toEqual(
        signed64(L('281474976710655')).length
      )
      expect(Encoder.sizeOfVarLong(L('281474976710656'))).toEqual(
        signed64(L('281474976710656')).length
      )
      expect(Encoder.sizeOfVarLong(L('-36028797018963968'))).toEqual(
        signed64(L('-36028797018963968')).length
      )
      expect(Encoder.sizeOfVarLong(L('-36028797018963969'))).toEqual(
        signed64(L('-36028797018963969')).length
      )
      expect(Encoder.sizeOfVarLong(L('-4611686018427387904'))).toEqual(
        signed64(L('-4611686018427387904')).length
      )
      expect(Encoder.sizeOfVarLong(L('-4611686018427387905'))).toEqual(
        signed64(L('-4611686018427387905')).length
      )
      expect(Encoder.sizeOfVarLong(Long.MIN_VALUE)).toEqual(signed64(Long.MIN_VALUE).length)
      expect(Encoder.sizeOfVarLong(Long.MAX_VALUE)).toEqual(signed64(Long.MAX_VALUE).length)
    })
  })
})
