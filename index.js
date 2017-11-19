const Kafka = require('./src')
const Compression = require('./src/protocol/message/compression')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
}
