const Encoder = require('../../../encoder')
const { SyncGroup: apiKey } = require('../../apiKeys')

/**
 * SyncGroup Request (Version: 4) => group_id generation_id member_id group_instance_id [group_assignment] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   group_assignment => member_id member_assignment TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     member_assignment => COMPACT_BYTES
 */

module.exports = ({
  groupId,
  generationId,
  memberId,
  groupInstanceId = null,
  groupAssignment,
}) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'SyncGroup',
  encode: async () => {
    return new Encoder()
      .writeUVarIntBytes()
      .writeUVarIntString(groupId)
      .writeInt32(generationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntArray(groupAssignment.map(encodeGroupAssignment))
      .writeUVarIntBytes()
  },
})

const encodeGroupAssignment = ({ memberId, memberAssignment }) => {
  return new Encoder()
    .writeUVarIntString(memberId)
    .writeUVarIntBytes(memberAssignment)
    .writeUVarIntBytes()
}
