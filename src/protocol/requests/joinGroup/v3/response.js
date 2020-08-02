const { parse: parseV0 } = require('../v0/response')
const { decode } = require('../v2/response')

/**
 * JoinGroup Response (Version: 3) => throttle_time_ms error_code generation_id group_protocol leader_id member_id [members]
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

module.exports = {
  decode,
  parse: parseV0,
}
