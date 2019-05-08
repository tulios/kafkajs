const Kafka = require('./src')
const PartitionAssigners = require('./src/consumer/assigners')
const AssignerProtocol = require('./src/consumer/assignerProtocol')
const Partitioners = require('./src/producer/partitioners')
const Compression = require('./src/protocol/message/compression')
const ResourceTypes = require('./src/protocol/resourceTypes')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka,
  PartitionAssigners,
  AssignerProtocol,
  Partitioners,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
  ResourceTypes,
}
