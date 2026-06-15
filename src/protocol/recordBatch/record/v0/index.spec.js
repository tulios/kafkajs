const Decoder = require('../../../decoder')
const RecordEncoder = require('./index')
const recordDecoder = require('./decoder')

describe('Protocol > RecordBatch > Record > v0 > encode', () => {
  test('encodes a record with a null header value', () => {
    const encoder = RecordEncoder({
      offsetDelta: 0,
      timestampDelta: 0,
      key: Buffer.from('key-0'),
      value: Buffer.from('value-0'),
      headers: { 'header-key-0': null },
    })

    const decoder = new Decoder(encoder.buffer)
    decoder.readVarInt() // length prefix

    const decoded = recordDecoder(decoder, {
      firstOffset: '0',
      firstTimestamp: '0',
      magicByte: 2,
    })

    expect(decoded.headers).toEqual({ 'header-key-0': null })
  })
})
