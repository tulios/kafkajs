const requestV0 = require('../v0/request')

/**
 * Metadata Request (Version: 2) => [topics]
 *   topics => STRING
 */

module.exports = ({ topics }) => Object.assign(requestV0({ topics }), { apiVersion: 2 })
