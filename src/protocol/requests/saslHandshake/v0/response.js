const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')

/**
 * SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms]
 *    error_code => INT16
 *    enabled_mechanisms => STRING
 */

const decode = rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
    enabledMechanisms: decoder.readArray(decoder => decoder.readString()),
  }
}

const parse = data => {
  if (failure(data.errorCode)) {
    throw new KafkaProtocolError(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
