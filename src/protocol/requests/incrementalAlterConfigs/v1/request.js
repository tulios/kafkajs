const Encoder = require('../../../encoder')
const { IncrementalAlterConfigs: apiKey } = require('../../apiKeys')

/**
 * IncrementalAlterConfigs Request (Version: 1) => [resources] validate_only TAG_BUFFER
 *   resources => resource_type resource_name [configs] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     configs => name config_operation value TAG_BUFFER
 *       name => COMPACT_STRING
 *       config_operation => INT8
 *       value => COMPACT_NULLABLE_STRING
 *   validate_only => BOOLEAN
 */

module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'IncrementalAlterConfigs',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(resources.map(encodeResource))
      .writeBoolean(validateOnly)
      .writeUVarIntBytes()
  },
})

const encodeResource = ({ type, name, configEntries }) => {
  return new Encoder()
    .writeInt8(type)
    .writeUVarIntString(name)
    .writeUVarIntArray(configEntries.map(encodeConfigEntry))
    .writeUVarIntBytes()
}

const encodeConfigEntry = ({ name, configOperation, value }) => {
  return new Encoder()
    .writeUVarIntString(name)
    .writeInt8(configOperation)
    .writeUVarIntString(value)
    .writeUVarIntBytes()
}
