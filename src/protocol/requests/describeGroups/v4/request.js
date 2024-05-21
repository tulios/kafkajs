const requestV3 = require('../v3/request')

/**
 * DescribeGroups Request (Version: 4) => [group_ids] include_authorized_operations
 *   group_ids => STRING
 *   include_authorized_operations => BOOLEAN
 */

module.exports = ({ groupIds, includeAuthorizedOperations = true }) =>
  Object.assign(requestV3({ groupIds, includeAuthorizedOperations }), { apiVersion: 4 })
