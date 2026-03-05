const { decode, parse } = require('../v6/response')

/**
 * OffsetCommit Response (Version: 7) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 */
module.exports = {
  decode,
  parse,
}
