const zlib = require('zlib')

module.exports = {
  /**
   * @param {Encoder} encoder
   * @returns {Promise}
   */
  compress(encoder) {
    return new Promise((resolve, reject) => {
      zlib.gzip(encoder.buffer, (error, result) => {
        if (error) {
          reject(error)
        }

        resolve(result)
      })
    })
  },

  /**
   * @param {Buffer} buffer
   */
  decompress(buffer) {
    return zlib.unzipSync(buffer)
  },
}
