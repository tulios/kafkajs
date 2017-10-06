const Types = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
}

const Codecs = {
  [Types.GZIP]: () => require('./gzip'),
}

module.exports = {
  Types,
  Codecs,
}
