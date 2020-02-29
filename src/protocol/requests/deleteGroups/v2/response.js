const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { KafkaJSDeleteGroupsError } = require('../../../../errors')
/**
 * DeleteGroups Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
 * throttle_time_ms => INT32
 * results => group_id error_code TAG_BUFFER
 *  group_id => COMPACT_STRING
 *  error_code => INT16
 */

const decodeGroup = decoder => ({
  groupId: decoder.readVarIntString(),
  errorCode: decoder.readInt16(),
})

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const results = decoder.readArray(decodeGroup)

  const errors = []
  for (const result of results) {
    if (failure(result.errorCode)) {
      errors.push({
        groupId: result.groupId,
        error: createErrorFromCode(result.errorCode),
      })
    }
  }

  if (errors.length > 0) throw new KafkaJSDeleteGroupsError('Error in DeleteGroups', errors)

  return {
    throttleTimeMs,
    results,
  }
}

const parse = async data => {
  return data
}

module.exports = {
  decode,
  parse,
}
