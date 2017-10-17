const MessageDecoder = require('../message/decoder')

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
      const offset = decoder.readInt64().toString()
      const size = decoder.readInt32()
      const message = MessageDecoder(offset, size, decoder)
      messages.push(message)
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
