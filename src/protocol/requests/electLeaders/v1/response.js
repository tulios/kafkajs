const { decode, parse } = require('../v0/response')

/**
 * ElectLeaders Response (Version: 1) => throttle_time error_code [replica_election_results]
 *   throttle_time => INT32
 *   error_code => INT16
 *   replica_election_results => topic [partition_result]
 *     topic => STRING
 *     partition_result => partition_id error_code error_message
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 */

module.exports = {
  decode,
  parse,
}
