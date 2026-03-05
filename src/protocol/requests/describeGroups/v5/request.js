const Encoder = require('../../../encoder')
const { DescribeGroups: apiKey } = require('../../apiKeys')

/**
 * DescribeGroups Request (Version: 5) => [groups] include_authorized_operations TAG_BUFFER
 *   groups => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 */

module.exports = ({ groupIds, includeAuthorizedOperations = false }) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'DescribeGroups',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(groupIds.map(id => new Encoder().writeUVarIntString(id)))
      .writeBoolean(includeAuthorizedOperations)
      .writeUVarIntBytes()
  },
})
