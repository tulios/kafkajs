const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const MessageSet = require('../../../messageSet')
const { Types, Codecs } = require('../../../message/compression')

// Produce Request on or after v2 indicates the client can parse the timestamp field
// in the produce Response.

module.exports = ({ acks, timeout, compression = Types.None, topicData }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'Produce',
  encode: async () => {
    return new Encoder()
      .writeInt16(acks)
      .writeInt32(timeout)
      .writeArray(topicData.map(encodeTopic(compression)))
  },
})

const encodeTopic = compression => ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartitions(compression)))
}

const encodePartitions = compression => ({ partition, messages }) => {
  const messageSet = MessageSet({ messageVersion: 1, entries: messages })

  if (compression === Types.None) {
    return new Encoder()
      .writeInt32(partition)
      .writeInt32(messageSet.size())
      .writeEncoder(messageSet)
  }

  const codec = lookupCompressionCodec(compression)
  const compressedValue = codec.compress(messageSet)
  const compressedMessageSet = MessageSet({
    messageVersion: 1,
    compression,
    entries: [{ offset: 0, compression, value: compressedValue }],
  })

  return new Encoder()
    .writeInt32(partition)
    .writeInt32(compressedMessageSet.size())
    .writeEncoder(compressedMessageSet)
}

const lookupCompressionCodec = type => (Codecs[type] ? Codecs[type]() : null)
