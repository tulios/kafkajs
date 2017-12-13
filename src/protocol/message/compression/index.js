const { KafkaJSNotImplemented } = require('../../../errors')

const Types = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
}

const Codecs = {
  [Types.GZIP]: () => require('./gzip'),
  [Types.Snappy]: () => {
    throw new KafkaJSNotImplemented('Snappy compression not implemented')
  },
  [Types.LZ4]: () => {
    throw new KafkaJSNotImplemented('LZ4 compression not implemented')
  },
}

const lookupCodec = type => (Codecs[type] ? Codecs[type]() : null)
const lookupCodecByAttributes = attributes => {
  const codec = Codecs[attributes & 0x3]
  return codec ? codec() : null
}

module.exports = {
  Types,
  Codecs,
  lookupCodec,
  lookupCodecByAttributes,
}
