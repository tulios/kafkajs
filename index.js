const Kafka = require('./src')
const PartitionAssigners = require('./src/consumer/assigners')
const AssignerProtocol = require('./src/consumer/assignerProtocol')
const Partitioners = require('./src/producer/partitioners')
const Compression = require('./src/protocol/message/compression')
const ConfigResourceTypes = require('./src/protocol/configResourceTypes')
const ConfigOperationTypes = require('./src/protocol/configOperationTypes')
const ConfigSource = require('./src/protocol/configSource')
const AclResourceTypes = require('./src/protocol/aclResourceTypes')
const AclOperationTypes = require('./src/protocol/aclOperationTypes')
const AclPermissionTypes = require('./src/protocol/aclPermissionTypes')
const ResourcePatternTypes = require('./src/protocol/resourcePatternTypes')
const { isRebalancing, isKafkaJSError, ...errors } = require('./src/errors')
const { LEVELS } = require('./src/loggers')
const deadLetterQueue = require('./src/consumer/deadLetterQueue')

module.exports = {
  Kafka,
  PartitionAssigners,
  AssignerProtocol,
  Partitioners,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
  ConfigResourceTypes,
  ConfigOperationTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  ConfigSource,
  deadLetterQueue,
  ...errors,
}
