const Encoder = require('../../../encoder')
const { AddRaftVoter: apiKey } = require('../../apiKeys')

module.exports = ({
  clusterId = null,
  timeoutMs = 30000,
  voterId,
  voterDirectoryId,
  listeners = [],
}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'AddRaftVoter',
  encode: async () => {
    const encoder = new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(clusterId)
      .writeInt32(timeoutMs)
      .writeInt32(voterId)
      .writeUUID(voterDirectoryId)

    // listeners - COMPACT_ARRAY
    encoder.writeUVarInt(listeners.length + 1)
    listeners.forEach(({ name, host, port, securityProtocol }) => {
      encoder.writeUVarIntString(name)
      encoder.writeUVarIntString(host)
      encoder.writeInt16(port)
      encoder.writeInt16(securityProtocol || 0)
      encoder.writeUVarIntBytes() // struct TAG_BUFFER
    })

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
