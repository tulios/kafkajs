const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')
const flatten = require('../../../../utils/flatten')
const MessageSetDecoder = require('../../../messageSet/decoder')

/**
* Fetch Response (Version: 0) => [responses]
*   responses => topic [partition_responses]
*     topic => STRING
*     partition_responses => partition_header record_set
*       partition_header => partition error_code high_watermark
*         partition => INT32
*         error_code => INT16
*         high_watermark => INT64
*       record_set => RECORDS
*/

const partition = decoder => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  highWatermark: decoder.readInt64().toString(),
  messages: MessageSetDecoder(decoder),
})

const decode = rawData => {
  const decoder = new Decoder(rawData)
  const responses = decoder.readArray(decoder => ({
    topicName: decoder.readString(),
    partitions: decoder.readArray(partition),
  }))

  return {
    responses,
  }
}

const parse = data => {
  const partitionsWithError = data.responses.map(response => {
    return response.partitions.filter(partition => failure(partition.errorCode))
  })

  const errors = flatten(partitionsWithError)
  if (errors.length > 0) {
    const { errorCode } = errors[0]
    throw new KafkaProtocolError(errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
