const responseV2 = require('../v2/response')

/**
 * ListGroups Response (Version: 3) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 */

module.exports = responseV2
