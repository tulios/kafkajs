const Decoder = require('../../../decoder')
const { parse } = require('../v1/response')

/**
 * Starting in version 4 : new:  member group instance id
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
 *
 * DescribeGroups Response (Version: 4) => throttle_time_ms [groups]
 *   throttle_time_ms => INT32
 *   groups => error_code group_id state protocol_type protocol [members] authorized_operations
 *     error_code => INT16
 *     group_id => STRING
 *     state => STRING
 *     protocol_type => STRING
 *     protocol => STRING
 *     members => member_id group_instance_id client_id client_host member_metadata member_assignment
 *       member_id => STRING
 *       group_instance_id => NULLABLE_STRING // new
 *       client_id => STRING
 *       client_host => STRING
 *       member_metadata => BYTES
 *       member_assignment => BYTES
 *     authorized_operations => INT32
 */

const decoderMember = decoder => ({
  memberId: decoder.readString(),
  groupInstanceId: decoder.readString(),
  clientId: decoder.readString(),
  clientHost: decoder.readString(),
  memberMetadata: decoder.readBytes(),
  memberAssignment: decoder.readBytes(),
})

const decodeGroup = decoder => ({
  errorCode: decoder.readInt16(),
  groupId: decoder.readString(),
  state: decoder.readString(),
  protocolType: decoder.readString(),
  protocol: decoder.readString(),
  members: decoder.readArray(decoderMember),
  authorizedOperations: decoder.readInt32(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = decoder.readArray(decodeGroup)

  return {
    throttleTime,
    groups,
  }
}

module.exports = {
  decode,
  parse,
}
