const requestV0 = require('../v0/request')

/**
 * Metadata Request (Version: 3) => [topics]
 *   topics => STRING
 */

module.exports = ({ topics }) => Object.assign(requestV0({ topics }), { apiVersion: 3 })
