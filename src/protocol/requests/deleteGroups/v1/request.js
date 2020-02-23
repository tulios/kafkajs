const requestV0 = require('../v0/request')

/**
 * ListGroups Request (Version: 1)
 */

module.exports = groupIds => Object.assign(requestV0(groupIds), { apiVersion: 1 })
