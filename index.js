const Kafka = require('./src')
const PartitionAssigners = require('./src/consumer/assigners')
const AssignerProtocol = require('./src/consumer/assignerProtocol')
const Partitioners = require('./src/producer/partitioners')
const Compression = require('./src/protocol/message/compression')
const ResourceTypes = require('./src/protocol/resourceTypes')
const ConfigResourceTypes = require('./src/protocol/configResourceTypes')
const ConfigSource = require('./src/protocol/configSource')
const AclResourceTypes = require('./src/protocol/aclResourceTypes')
const AclOperationTypes = require('./src/protocol/aclOperationTypes')
const AclPermissionTypes = require('./src/protocol/aclPermissionTypes')
const ResourcePatternTypes = require('./src/protocol/resourcePatternTypes')
const Errors = require('./src/errors')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka,
  PartitionAssigners,
  AssignerProtocol,
  Partitioners,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
  /**
   * @deprecated
   * @see https://github.com/tulios/kafkajs/issues/649
   *
   * Use ConfigResourceTypes or AclResourceTypes instead.
   */
  ResourceTypes,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  ConfigSource,
  ...Errors,
}
