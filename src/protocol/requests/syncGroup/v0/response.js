const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')

/**
 * SyncGroup Response (Version: 0) => error_code member_assignment
 *   error_code => INT16
 *   member_assignment => BYTES
 */

const decode = rawData => {
  const decoder = new Decoder(rawData)
  return {
    errorCode: decoder.readInt16(),
    memberAssignment: decoder.readBytes(),
  }
}

const parse = data => {
  if (failure(data.errorCode)) {
    throw new KafkaProtocolError(data.errorCode)
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
