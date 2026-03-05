const Encoder = require('../../../encoder')
const { RemoveRaftVoter: apiKey } = require('../../apiKeys')

module.exports = ({ clusterId = null, timeoutMs = 30000, voterId, voterDirectoryId }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'RemoveRaftVoter',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(clusterId)
      .writeInt32(timeoutMs)
      .writeInt32(voterId)
      .writeUUID(voterDirectoryId)
      .writeUVarIntBytes() // request TAG_BUFFER
  },
})
