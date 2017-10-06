const zlib = require('zlib')

module.exports = {
  compress(encoder) {
    return zlib.gzipSync(encoder.buffer)
  },

  decompress() {
    return zlib.gunzipSync
  },
}
