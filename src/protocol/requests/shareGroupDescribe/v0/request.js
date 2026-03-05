const Encoder = require('../../../encoder')
const { ShareGroupDescribe: apiKey } = require('../../apiKeys')

module.exports = ({ groupIds, includeAuthorizedOperations = false }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ShareGroupDescribe',
  encode: async () => {
    const encoder = new Encoder().writeUVarIntBytes() // header TAG_BUFFER

    // groupIds - COMPACT_ARRAY of COMPACT_STRING
    encoder.writeUVarInt(groupIds.length + 1)
    groupIds.forEach(id => encoder.writeUVarIntString(id))
    encoder.writeBoolean(includeAuthorizedOperations)
    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
