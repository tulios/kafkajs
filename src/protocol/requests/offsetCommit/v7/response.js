const { parse, decode } = require('../v6/response')

/**
 * OffsetCommit Response (Version: 7) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
module.exports = {
  decode,
  parse,
}
