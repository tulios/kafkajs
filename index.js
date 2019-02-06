const createClientClass = require('./src')
const PartitionAssigners = require('./src/consumer/assigners')
const AssignerProtocol = require('./src/consumer/assignerProtocol')
const Compression = require('./src/protocol/message/compression')
const ResourceTypes = require('./src/protocol/resourceTypes')
const nodeSocketFactory = require('./src/network/socketFactory')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka: createClientClass(nodeSocketFactory),
  PartitionAssigners,
  AssignerProtocol,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
  ResourceTypes,
}
