const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')
const { DEFAULT_CONFIG } = require('../../../configSource')

/**
 * DescribeConfigs Response (Version: 4) => throttle_time_ms [resources] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name [config_entries] TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     config_entries => config_name config_value read_only config_source is_sensitive [config_synonyms] TAG_BUFFER
 *       config_name => COMPACT_STRING
 *       config_value => COMPACT_NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *       config_synonyms => config_name config_value config_source TAG_BUFFER
 *         config_name => COMPACT_STRING
 *         config_value => COMPACT_NULLABLE_STRING
 *         config_source => INT8
 */

const decodeSynonyms = decoder => {
  const synonym = {
    configName: decoder.readUVarIntString(),
    configValue: decoder.readUVarIntString(),
    configSource: decoder.readInt8(),
  }
  decoder.readTaggedFields()
  return synonym
}

const decodeConfigEntries = decoder => {
  const configName = decoder.readUVarIntString()
  const configValue = decoder.readUVarIntString()
  const readOnly = decoder.readBoolean()
  const configSource = decoder.readInt8()
  const isSensitive = decoder.readBoolean()
  const configSynonyms = decoder.readUVarIntArray(decodeSynonyms)
  decoder.readTaggedFields()

  return {
    configName,
    configValue,
    readOnly,
    isDefault: configSource === DEFAULT_CONFIG,
    configSource,
    isSensitive,
    configSynonyms,
  }
}

const decodeResources = decoder => {
  const resource = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
    configEntries: decoder.readUVarIntArray(decodeConfigEntries),
  }
  decoder.readTaggedFields()
  return resource
}

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const resources = decoder.readUVarIntArray(decodeResources)
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    resources,
  }
}

module.exports = {
  decode,
  parse: parseV0,
}
