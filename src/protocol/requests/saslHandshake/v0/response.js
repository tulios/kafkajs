const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms]
 *    error_code => INT16
 *    enabled_mechanisms => STRING
 */

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
    enabledMechanisms: decoder.readArray(decoder => decoder.readString()),
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
