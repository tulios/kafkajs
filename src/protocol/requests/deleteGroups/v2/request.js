const Encoder = require('../../../encoder')
const { DeleteGroups: apiKey } = require('../../apiKeys')

/**
 * DeleteGroups Request (Version: 2) => [groups_names] TAG_BUFFER
 *   groups_names => COMPACT_STRING
 */

/**
 */
module.exports = groupIds => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DeleteGroups',
  encode: async () => {
    return new Encoder().writeArray(groupIds.map(encodeGroups))
  },
})

const encodeGroups = group => {
  return new Encoder().writeVarIntString(group)
}
