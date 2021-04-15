const Long = require('../utils/long')

const Encoder = require('./encoder')
const Decoder = require('./decoder')

const MAX_SAFE_POSITIVE_SIGNED_INT = 2147483647
const MIN_SAFE_NEGATIVE_SIGNED_INT = -2147483648

const MAX_SAFE_UNSIGNED_INT = 4294967295
const MIN_SAFE_UNSIGNED_INT = 0

describe('Protocol > Encoder', () => {
  const signed32 = number => new Encoder().writeVarInt(number).buffer
  const decode32 = buffer => new Decoder(buffer).readVarInt()

  const unsigned32 = number => new Encoder().writeUVarInt(number).buffer
  const decode32u = buffer => new Decoder(buffer).readUVarInt()

  const signed64 = number => new Encoder().writeVarLong(number).buffer
  const decode64 = buffer => new Decoder(buffer).readVarLong()

  const encodeDouble = number => new Encoder().writeDouble(number).buffer
  const decodeDouble = buffer => new Decoder(buffer).readDouble()

  const ustring = string => new Encoder().writeUVarIntString(string).buffer
  const decodeUString = buffer => new Decoder(buffer).readUVarIntString()

  const ubytes = bytes => new Encoder().writeUVarIntBytes(bytes).buffer
  const decodeUBytes = buffer => new Decoder(buffer).readUVarIntBytes()

  const uarray = array => new Encoder().writeUVarIntArray(array).buffer

  const B = (...args) => Buffer.from(args)
  const L = value => Long.fromString(`${value}`)

  describe('Unsigned VarInt Array', () => {
    const encodeUVarInt = number => new Encoder().writeUVarInt(number)
    const array = [7681, 823, 9123, 9812, 3219]
    test('encode uvarint array', () => {
      expect(uarray(array.map(encodeUVarInt))).toEqual(
        B(0x06, 0x81, 0x3c, 0xb7, 0x06, 0xa3, 0x47, 0xd4, 0x4c, 0x93, 0x19)
      )
    })

    test('decode uvarint array', () => {
      const decodeUVarInt = decoder => decoder.readUVarInt()
      const encodedArray = uarray(array.map(encodeUVarInt))
      const decoder = new Decoder(encodedArray)
      expect(decoder.readUVarIntArray(decodeUVarInt)).toEqual(array)
    })
  })

  describe('Unsigned VarInt Bytes', () => {
    test('encode uvarint bytes', () => {
      expect(ubytes(null)).toEqual(B(0x00))
      expect(ubytes('')).toEqual(B(0x01))
      expect(ubytes('kafkajs')).toEqual(B(0x08, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x6a, 0x73))
    })

    test('decode uvarint bytes', () => {
      expect(decodeUBytes(ubytes(null))).toEqual(null)
      expect(decodeUBytes(ubytes(''))).toEqual(B())
      expect(decodeUBytes(ubytes('kafkajs'))).toEqual(B(0x6b, 0x61, 0x66, 0x6b, 0x61, 0x6a, 0x73))
    })
  })

  describe('Unsigned VarInt String', () => {
    test('encode uvarint string', () => {
      expect(ustring(null)).toEqual(B(0x00))
      expect(ustring('')).toEqual(B(0x01))
      expect(ustring('kafkajs')).toEqual(B(0x08, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x6a, 0x73))
    })

    test('decode uvarint string', () => {
      expect(decodeUString(ustring(null))).toEqual(null)
      expect(decodeUString(ustring(''))).toEqual('')
      expect(decodeUString(ustring('kafkajs'))).toEqual('kafkajs')
    })
  })

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

  describe('double', () => {
    test('encode double', () => {
      expect(encodeDouble(-3.1415926535897932)).toEqual(
        B(0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18)
      )
      expect(encodeDouble(-0.3333333333333333)).toEqual(
        B(0xbf, 0xd5, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55)
      )
      expect(encodeDouble(-59.82946381)).toEqual(B(0xc0, 0x4d, 0xea, 0x2b, 0xde, 0xc0, 0x95, 0x31))
      expect(encodeDouble(-1.5)).toEqual(B(0xbf, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00))
      expect(encodeDouble(0.0)).toEqual(B(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00))
      expect(encodeDouble(1.5)).toEqual(B(0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00))
      expect(encodeDouble(59.82946381)).toEqual(B(0x40, 0x4d, 0xea, 0x2b, 0xde, 0xc0, 0x95, 0x31))
      expect(encodeDouble(0.3333333333333333)).toEqual(
        B(0x3f, 0xd5, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55)
      )
      expect(encodeDouble(3.1415926535897932)).toEqual(
        B(0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18)
      )
    })
    test('decode double', () => {
      expect(decodeDouble(encodeDouble(-3.1415926535897932))).toEqual(-3.1415926535897932)
      expect(decodeDouble(encodeDouble(-0.3333333333333333))).toEqual(-0.3333333333333333)
      expect(decodeDouble(encodeDouble(-59.82946381))).toEqual(-59.82946381)
      expect(decodeDouble(encodeDouble(-1.5))).toEqual(-1.5)
      expect(decodeDouble(encodeDouble(0.0))).toEqual(0.0)
      expect(decodeDouble(encodeDouble(1.5))).toEqual(1.5)
      expect(decodeDouble(encodeDouble(59.82946381))).toEqual(59.82946381)
      expect(decodeDouble(encodeDouble(0.3333333333333333))).toEqual(0.3333333333333333)
      expect(decodeDouble(encodeDouble(3.1415926535897932))).toEqual(3.1415926535897932)
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

    test('decode signed int32 numbers', () => {
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

  describe('uvarint', () => {
    test('encode unsigned int32 numbers', () => {
      expect(unsigned32(0)).toEqual(B(0x00))
      expect(unsigned32(1)).toEqual(B(0x01))
      expect(unsigned32(127)).toEqual(B(0x7f))
      expect(unsigned32(128)).toEqual(B(0x80, 0x01))
      expect(unsigned32(8192)).toEqual(B(0x80, 0x40))
      expect(unsigned32(16383)).toEqual(B(0xff, 0x7f))
      expect(unsigned32(16384)).toEqual(B(0x80, 0x80, 0x01))
      expect(unsigned32(2097151)).toEqual(B(0xff, 0xff, 0x7f))
      expect(unsigned32(2097152)).toEqual(B(0x80, 0x80, 0x80, 0x01))
      expect(unsigned32(134217728)).toEqual(B(0x80, 0x80, 0x80, 0x40))
      expect(unsigned32(268435455)).toEqual(B(0xff, 0xff, 0xff, 0x7f))
    })

    test('encode unsigned int32 boundaries', () => {
      expect(unsigned32(MAX_SAFE_UNSIGNED_INT)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x0f))
      expect(unsigned32(MIN_SAFE_UNSIGNED_INT)).toEqual(B(0x00))
    })

    test('decode unsigned int32 numbers', () => {
      expect(decode32u(unsigned32(0))).toEqual(0)
      expect(decode32u(unsigned32(1))).toEqual(1)
      expect(decode32u(unsigned32(127))).toEqual(127)
      expect(decode32u(unsigned32(128))).toEqual(128)
      expect(decode32u(unsigned32(8192))).toEqual(8192)
      expect(decode32u(unsigned32(16383))).toEqual(16383)
      expect(decode32u(unsigned32(16384))).toEqual(16384)
      expect(decode32u(unsigned32(2097151))).toEqual(2097151)
      expect(decode32u(unsigned32(134217728))).toEqual(134217728)
      expect(decode32u(unsigned32(268435455))).toEqual(268435455)
    })

    test('decode unsigned int32 boundaries', () => {
      expect(() => decode32u(B(0xff, 0xff, 0xff, 0xff, 0xff, 0x01))).toThrow()
      expect(decode32u(unsigned32(MAX_SAFE_UNSIGNED_INT))).toEqual(MAX_SAFE_UNSIGNED_INT)
      expect(decode32u(unsigned32(MIN_SAFE_UNSIGNED_INT))).toEqual(MIN_SAFE_UNSIGNED_INT)
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
      expect(() =>
        decode64(B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01))
      ).toThrow()
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

  describe('resizing', () => {
    it('copies existing content when resizing', () => {
      const encoder = new Encoder(4)
      encoder.writeBuffer(B(1, 2, 3, 4))
      encoder.writeBuffer(B(5, 6, 7, 8))
      expect(encoder.buffer).toEqual(B(1, 2, 3, 4, 5, 6, 7, 8))
    })
    it('obeys offset when resizing', () => {
      const encoder = new Encoder(4)
      // Only two bytes in, ...
      encoder.writeBuffer(B(1, 2))
      // ... but this write will require resizing
      encoder.writeBuffer(B(5, 6, 7, 8))
      expect(encoder.buffer).toEqual(B(1, 2, 5, 6, 7, 8))
    })
  })
})
