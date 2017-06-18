const Encoder = require('../../../encoder')
const { definition: { Metadata: apiKey } } = require('../../../apiKeys')

/**
 * Metadata Request (Version: 0) => [topics]
 *   topics => STRING
 */

module.exports = topics => ({
  apiKey,
  apiVersion: 0,
  protocol: () => {
    return new Encoder().writeArray(topics)
  },
})
