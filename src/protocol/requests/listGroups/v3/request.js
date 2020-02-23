const requestV2 = require('../v2/request')

/**
 * ListGroups Request (Version: 3)
 */

module.exports = () => Object.assign(requestV2(), { apiVersion: 3 })
