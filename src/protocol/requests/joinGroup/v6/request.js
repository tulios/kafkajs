const Encoder = require('../../../encoder')
const { JoinGroup: apiKey } = require('../../apiKeys')

/**
 * JoinGroup Request (Version: 6) => group_id session_timeout rebalance_timeout member_id group_instance_id protocol_type [group_protocols] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   protocol_type => COMPACT_STRING
 *   group_protocols => protocol_name protocol_metadata TAG_BUFFER
 *     protocol_name => COMPACT_STRING
 *     protocol_metadata => COMPACT_BYTES
 */

module.exports = ({
  groupId,
  sessionTimeout,
  rebalanceTimeout,
  memberId,
  groupInstanceId = null,
  protocolType,
  groupProtocols,
}) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'JoinGroup',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(groupId)
      .writeInt32(sessionTimeout)
      .writeInt32(rebalanceTimeout)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntString(protocolType)
      .writeUVarIntArray(groupProtocols.map(encodeGroupProtocols))
      .writeUVarIntBytes()
  },
})

const encodeGroupProtocols = ({ name, metadata = Buffer.alloc(0) }) => {
  return new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntBytes(metadata)
    .writeUVarIntBytes()
}
