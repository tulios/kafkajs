const Encoder = require('../../../encoder')
const { UpdateRaftVoter: apiKey } = require('../../apiKeys')

module.exports = ({
  clusterId = null,
  currentLeaderEpoch,
  voterId,
  voterDirectoryId,
  listeners = [],
  kRaftVersionFeature = null,
}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'UpdateRaftVoter',
  encode: async () => {
    const encoder = new Encoder()
      .writeUVarIntBytes() // header TAG_BUFFER
      .writeUVarIntString(clusterId)
      .writeInt32(currentLeaderEpoch)
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

    // kRaftVersionFeature
    if (kRaftVersionFeature) {
      encoder.writeInt16(kRaftVersionFeature.minSupportedVersion)
      encoder.writeInt16(kRaftVersionFeature.maxSupportedVersion)
    } else {
      encoder.writeInt16(0)
      encoder.writeInt16(0)
    }
    encoder.writeUVarIntBytes() // kRaftVersionFeature TAG_BUFFER

    encoder.writeUVarIntBytes() // request TAG_BUFFER
    return encoder
  },
})
