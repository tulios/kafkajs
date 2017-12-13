const Long = require('long')
const Decoder = require('../decoder')
const MessageDecoder = require('../message/decoder')
const { lookupCodecByAttributes } = require('../message/compression')

/**
 * MessageSet => [Offset MessageSize Message]
 *  Offset => int64
 *  MessageSize => int32
 *  Message => Bytes
 */

module.exports = async primaryDecoder => {
  const messages = []
  const messageSetSize = primaryDecoder.readInt32()
  const messageSetDecoder = primaryDecoder.slice(messageSetSize)

  while (messageSetDecoder.offset < messageSetSize) {
    try {
      const message = EntryDecoder(messageSetDecoder)
      const codec = lookupCodecByAttributes(message.attributes)

      if (codec) {
        const buffer = await codec.decompress(message.value)
        messages.push(...EntriesDecoder(new Decoder(buffer), message))
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

  primaryDecoder.forward(messageSetSize)
  return messages
}

const EntriesDecoder = (decoder, compressedMessage) => {
  const messages = []

  while (decoder.offset < decoder.buffer.length) {
    messages.push(EntryDecoder(decoder))
  }

  if (compressedMessage.magicByte > 0 && compressedMessage.offset >= 0) {
    const compressedOffset = Long.fromValue(compressedMessage.offset)
    const lastMessageOffset = Long.fromValue(messages[messages.length - 1].offset)
    const baseOffset = compressedOffset - lastMessageOffset

    for (let message of messages) {
      message.offset = Long.fromValue(message.offset)
        .add(baseOffset)
        .toString()
    }
  }

  return messages
}

const EntryDecoder = decoder => {
  const offset = decoder.readInt64().toString()
  const size = decoder.readInt32()
  return MessageDecoder(offset, size, decoder)
}
