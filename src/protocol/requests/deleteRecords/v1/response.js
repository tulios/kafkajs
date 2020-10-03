const responseV0 = require('../v0/response')

/**
 * DeleteRecords Response (Version: 1) => throttle_time_ms [topics]
 *  throttle_time_ms => INT32
 *  topics => name [partitions]
 *    name => STRING
 *    partitions => partition_index low_watermark error_code
 *      partition_index => INT32
 *      low_watermark => INT64
 *      error_code => INT16
 */
module.exports = responseV0
