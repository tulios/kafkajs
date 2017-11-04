const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * LeaveGroup Response (Version: 0) => error_code
 *   error_code => INT16
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
  }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
