const Encoder = require('../../../encoder')
const { JoinGroup: apiKey } = require('../../apiKeys')

/**
 * JoinGroup Request (Version: 5) => group_id session_timeout rebalance_timeout member_id group_instance_id protocol_type [group_protocols]
 *   group_id => STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING
 *   protocol_type => STRING
 *   group_protocols => protocol_name protocol_metadata
 *     protocol_name => STRING
 *     protocol_metadata => BYTES
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
  apiVersion: 5,
  apiName: 'JoinGroup',
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeInt32(sessionTimeout)
      .writeInt32(rebalanceTimeout)
      .writeString(memberId)
      .writeString(groupInstanceId)
      .writeString(protocolType)
      .writeArray(groupProtocols.map(encodeGroupProtocols))
  },
})

const encodeGroupProtocols = ({ name, metadata = Buffer.alloc(0) }) => {
  return new Encoder().writeString(name).writeBytes(metadata)
}
