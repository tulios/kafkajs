const requestV6 = require('../v6/request')

/**
 * Metadata Request (Version: 7) => [topics] allow_auto_topic_creation
 *   topics => STRING
 *   allow_auto_topic_creation => BOOLEAN
 */

module.exports = ({ topics, allowAutoTopicCreation = true }) =>
  Object.assign(requestV6({ topics, allowAutoTopicCreation }), { apiVersion: 7 })
