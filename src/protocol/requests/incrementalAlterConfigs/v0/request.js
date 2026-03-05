const Encoder = require('../../../encoder')
const { IncrementalAlterConfigs: apiKey } = require('../../apiKeys')

/**
 * IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only
 *   resources => resource_type resource_name [configs]
 *     resource_type => INT8
 *     resource_name => STRING
 *     configs => name config_operation value
 *       name => STRING
 *       config_operation => INT8
 *       value => NULLABLE_STRING
 *   validate_only => BOOLEAN
 *
 * config_operation:
 *   0 = SET
 *   1 = DELETE
 *   2 = APPEND
 *   3 = SUBTRACT
 */

module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'IncrementalAlterConfigs',
  encode: async () => {
    return new Encoder().writeArray(resources.map(encodeResource)).writeBoolean(validateOnly)
  },
})

const encodeResource = ({ type, name, configEntries }) => {
  return new Encoder()
    .writeInt8(type)
    .writeString(name)
    .writeArray(configEntries.map(encodeConfigEntry))
}

const encodeConfigEntry = ({ name, configOperation, value }) => {
  return new Encoder()
    .writeString(name)
    .writeInt8(configOperation)
    .writeString(value)
}
