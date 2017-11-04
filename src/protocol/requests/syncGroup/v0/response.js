const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * SyncGroup Response (Version: 0) => error_code member_assignment
 *   error_code => INT16
 *   member_assignment => BYTES
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
    memberAssignment: decoder.readBytes(),
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return {
    errorCode: data.errorCode,
    memberAssignment: JSON.parse(data.memberAssignment.toString()),
  }
}

module.exports = {
  decode,
  parse,
}
