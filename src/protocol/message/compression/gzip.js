const zlib = require('zlib')

module.exports = {
  compress(encoder) {
    return zlib.gzipSync(encoder.buffer)
  },

  decompress(buffer) {
    return zlib.unzipSync(buffer)
  },
}
