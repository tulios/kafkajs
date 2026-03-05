const Encoder = require('../../../encoder')
const { AlterConfigs: apiKey } = require('../../apiKeys')

/**
 * AlterConfigs Request (Version: 2) => [resources] validate_only TAG_BUFFER
 *   resources => resource_type resource_name [config_entries] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     config_entries => config_name config_value TAG_BUFFER
 *       config_name => COMPACT_STRING
 *       config_value => COMPACT_NULLABLE_STRING
 *   validate_only => BOOLEAN
 */

module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'AlterConfigs',
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
    .writeUVarIntArray(configEntries.map(encodeConfigEntries))
    .writeUVarIntBytes()
}

const encodeConfigEntries = ({ name, value }) => {
  return new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntString(value)
    .writeUVarIntBytes()
}
