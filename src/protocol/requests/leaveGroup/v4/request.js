const Encoder = require('../../../encoder')
const { LeaveGroup: apiKey } = require('../../apiKeys')

/**
 * LeaveGroup Request (Version: 4) => group_id [members] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   members => member_id group_instance_id TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     group_instance_id => COMPACT_NULLABLE_STRING
 */

module.exports = ({ groupId, members }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'LeaveGroup',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(members.map(member => encodeMember(member)))
      .writeUVarIntBytes()
  },
})

const encodeMember = ({ memberId, groupInstanceId = null }) => {
  return new Encoder()
    .writeUVarIntString(memberId)
    .writeUVarIntString(groupInstanceId)
    .writeUVarIntBytes()
}
