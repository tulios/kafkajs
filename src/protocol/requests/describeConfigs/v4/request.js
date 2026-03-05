const Encoder = require('../../../encoder')
const { DescribeConfigs: apiKey } = require('../../apiKeys')

/**
 * DescribeConfigs Request (Version: 4) => [resources] include_synonyms include_documentation TAG_BUFFER
 *   resources => resource_type resource_name [config_names] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     config_names => COMPACT_STRING
 *   include_synonyms => BOOLEAN
 *   include_documentation => BOOLEAN
 */

module.exports = ({ resources, includeSynonyms = false, includeDocumentation = false }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'DescribeConfigs',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(resources.map(encodeResource))
      .writeBoolean(includeSynonyms)
      .writeBoolean(includeDocumentation)
      .writeUVarIntBytes()
  },
})

const encodeResource = ({ type, name, configNames = [] }) => {
  return new Encoder()
    .writeInt8(type)
    .writeUVarIntString(name)
    .writeUVarIntArray(
      configNames.length > 0 ? configNames.map(n => new Encoder().writeUVarIntString(n)) : null
    )
    .writeUVarIntBytes()
}
