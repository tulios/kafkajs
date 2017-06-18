const Encoder = require('../../../encoder')
const { definition: { ApiVersions: apiKey } } = require('../../../apiKeys')

/**
 * ApiVersionRequest => ApiKeys
 */

module.exports = () => ({
  apiKey,
  apiVersion: 0,
  protocol: () => new Encoder(),
})
