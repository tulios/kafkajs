const { decode } = require('../v3/response')
const { failure, createErrorFromCode } = require('../../../error')

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

const parse = async data => {
  if (failure(data.errorCode)) {
    const error = createErrorFromCode(data.errorCode)
    if (error.type === 'MEMBER_ID_REQUIRED') {
      error.memberId = data.memberId
    }

    throw error
  }

  return data
}

module.exports = {
  decode,
  parse,
}
