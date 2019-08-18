#!/usr/bin/env node

const Benchmark = require('benchmark')
const Encoder = require('../src/protocol/encoder')
const EncoderV2 = require('../src/protocol/encoderV2')

const suite = new Benchmark.Suite('Encode')

const doEncoding = async encoder => {
  encoder
    .writeInt8(127)
    .writeInt16(32767)
    .writeInt32(2147483647)
    .writeUInt32(2147483647)
    .writeInt64(9223372036854775808)
    .writeBoolean(true)
    .writeString('An ounce of performance is worth a pound of promises.')
    .writeVarIntString(
      'Don’t worry if it doesn’t work right. If everything did, you’d be out of a job.'
    )
    .writeBytes(Buffer.from([0, 1, 1, 1, 0, 0, 1, 1]))
    .writeVarIntBytes(Buffer.from([0, 1, 1, 1, 0, 0, 1, 1]))

  encoder.writeEncoder(encoder)
  encoder.writeEncoderArray([encoder, encoder])
  encoder.writeBuffer(encoder.buffer)
}

suite
  .add('Encode with native buffer', () => {
    doEncoding(new Encoder())
  })
  .add('Encode with expanding buffer', () => {
    doEncoding(new EncoderV2())
  })
  .on('cycle', function(event) {
    console.log(String(event.target))
  })
  .on('complete', function() {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()
