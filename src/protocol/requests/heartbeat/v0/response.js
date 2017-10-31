const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')

/**
 * Heartbeat Response (Version: 0) => error_code
 *   error_code => INT16
 */

const decode = rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
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
