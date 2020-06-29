const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * JoinGroup Response (Version: 4) => throttle_time_ms error_code generation_id group_protocol leader_id member_id [members]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   generation_id => INT32
 *   group_protocol => STRING
 *   leader_id => STRING
 *   member_id => STRING
 *   members => member_id member_metadata
 *     member_id => STRING
 *     member_metadata => BYTES
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  return {
    throttleTime,
    errorCode,
    generationId: decoder.readInt32(),
    groupProtocol: decoder.readString(),
    leaderId: decoder.readString(),
    memberId: decoder.readString(),
    members: decoder.readArray(decoder => ({
      memberId: decoder.readString(),
      memberMetadata: decoder.readBytes(),
    })),
  }
}

const parse = async data => {
  // KIP-394: Require member.id for initial join group request
  if (data.errorCode !== 79 && failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
