const Types = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
}

const Codecs = {
  [Types.GZIP]: () => require('./gzip'),
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
