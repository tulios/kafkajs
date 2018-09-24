const { parse: parseV1, decode: decodeV1 } = require('../v1/response')

/**
 * ApiVersions Response (Version: 2) => error_code [api_versions] throttle_time_ms
 *   error_code => INT16
 *   api_versions => api_key min_version max_version
 *     api_key => INT16
 *     min_version => INT16
 *     max_version => INT16
 *   throttle_time_ms => INT32
 */

module.exports = {
  parse: parseV1,
  decode: decodeV1,
}
