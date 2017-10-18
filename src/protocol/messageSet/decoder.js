const Decoder = require('../decoder')
const MessageDecoder = require('../message/decoder')
const { Codecs } = require('../message/compression')

/**
 * MessageSet => [Offset MessageSize Message]
 *  Offset => int64
 *  MessageSize => int32
 *  Message => Bytes
 */

module.exports = decoder => {
  const messages = []
  const messageSetSize = decoder.readInt32()
  const bytesToRead = decoder.offset + messageSetSize

  while (decoder.offset < bytesToRead) {
    try {
      const message = EntryDecoder(decoder)
      const codec = getCompressionCodec(message)

      if (codec) {
        const buffer = codec.decompress(message.value)
        messages.push(...EntriesDecoder(new Decoder(buffer)))
      } else {
        messages.push(message)
      }
    } catch (e) {
      if (e.name === 'KafkaJSPartialMessageError') {
        // We tried to decode a partial message, it means that minBytes
        // is probably too low
        break
      }

      throw e
    }
  }

  return messages
}

const EntriesDecoder = decoder => {
  const messages = []
  while (decoder.offset < decoder.buffer.length) {
    messages.push(EntryDecoder(decoder))
  }
  return messages
}

const EntryDecoder = decoder => {
  const offset = decoder.readInt64().toString()
  const size = decoder.readInt32()
  return MessageDecoder(offset, size, decoder)
}

const getCompressionCodec = message => {
  const codec = Codecs[message.attributes & 0x3]
  return codec ? codec() : null
}
