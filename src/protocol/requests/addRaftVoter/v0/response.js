const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

const decode = async rawData => {
  const decoder = new Decoder(rawData)
  decoder.readTaggedFields()
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const currentLeader = (() => {
    const leaderId = decoder.readInt32()
    const host = decoder.readUVarIntString()
    const port = decoder.readInt32()
    decoder.readTaggedFields()
    return { leaderId, host, port }
  })()

  return { throttleTime, errorCode, errorMessage, currentLeader }
}

const parse = async data => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }
  return data
}

module.exports = { decode, parse }
