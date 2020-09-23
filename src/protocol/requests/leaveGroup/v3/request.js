const Encoder = require('../../../encoder')
const { LeaveGroup: apiKey } = require('../../apiKeys')

/**
 * LeaveGroup Request (Version: 3) => group_id [members]
 *   group_id => STRING
 *   members => member_id group_instance_id
 *     member_id => STRING
 *     group_instance_id => NULLABLE_STRING
 */

module.exports = ({ groupId, members }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'LeaveGroup',
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeArray(members.map(member => encodeMember(member)))
  },
})

const encodeMember = ({ memberId, groupInstanceId = null }) => {
  return new Encoder().writeString(memberId).writeString(groupInstanceId)
}
