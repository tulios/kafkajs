const Encoder = require('../../../encoder')
const { ListGroups: apiKey } = require('../../apiKeys')

/**
 * ListGroups Request (Version: 4) => [states_filter] TAG_BUFFER
 *   states_filter => COMPACT_STRING
 */

module.exports = ({ statesFilter = [] } = {}) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'ListGroups',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntArray(statesFilter.map(s => new Encoder().writeUVarIntString(s)))
      .writeUVarIntBytes()
  },
})
