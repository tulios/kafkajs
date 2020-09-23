const { parse, decode } = require('../v1/response')

/**
 * LeaveGroup Response (Version: 2) => throttle_time_ms error_code
 *   throttle_time_ms => INT32
 *   error_code => INT16
 */
module.exports = {
  decode,
  parse,
}
