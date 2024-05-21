const Encoder = require('../../../encoder')
const { DescribeGroups: apiKey } = require('../../apiKeys')

/**
 * DescribeGroups Request (Version: 3) => [group_ids] include_authorized_operations
 *   group_ids => STRING
 *   include_authorized_operations => BOOLEAN
 */

module.exports = ({ groupIds, includeAuthorizedOperations = true }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'DescribeGroups',
  encode: async () => {
    return new Encoder().writeArray(groupIds).writeBoolean(includeAuthorizedOperations)
  },
})
