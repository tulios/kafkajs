const Decoder = require('../../../decoder')
const { failure, KafkaProtocolError } = require('../../../error')

/**
 * v0
 * ProduceResponse => [TopicName [Partition ErrorCode Offset]]
 *   TopicName => string
 *   Partition => int32
 *   ErrorCode => int16
 *   Offset => int64
 */

const partition = decoder => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  offset: decoder.readInt64().toString(),
})

const decode = rawData => {
  const decoder = new Decoder(rawData)
  const topics = decoder.readArray(decoder => ({
    topicName: decoder.readString(),
    partitions: decoder.readArray(partition),
  }))

  return {
    topics,
  }
}

const flatten = arrays => [].concat.apply([], arrays)

// {"topics":[{"topicName":"topic1","partitions":[{"partition":0,"errorCode":0,"offset":"4"}]}]}
const parse = data => {
  const partitionsWithError = data.topics.map(topic => {
    return topic.partitions.filter(partition => failure(partition.errorCode))
  })

  const errors = flatten(partitionsWithError)
  if (errors.length > 0) {
    const { errorCode } = error[0]
    throw new KafkaProtocolError(errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
