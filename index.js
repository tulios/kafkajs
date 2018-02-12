const Kafka = require('./src')
const PartitionAssigners = require('./src/consumer/assigners')
const Compression = require('./src/protocol/message/compression')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka,
  PartitionAssigners,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
}
