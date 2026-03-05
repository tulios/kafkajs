const Encoder = require('../../../encoder')
const { ApiVersions: apiKey } = require('../../apiKeys')

/**
 * ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
 *   client_software_name => COMPACT_STRING
 *   client_software_version => COMPACT_STRING
 */

module.exports = ({ clientSoftwareName = 'kafkajs', clientSoftwareVersion = '' } = {}) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'ApiVersions',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(clientSoftwareName)
      .writeUVarIntString(clientSoftwareVersion)
      .writeUVarIntBytes()
  },
})
