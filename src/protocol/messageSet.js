const Encoder = require('./encoder')
const Message = require('./message')

/**
 * MessageSet => [Offset MessageSize Message]
 *  Offset => int64
 *  MessageSize => int32
 *  Message => Bytes
 */

/**
 * [
 *   { key: "<value>", value: "<value>" },
 *   { key: "<value>", value: "<value>" },
 * ]
 */
module.exports = ({ entries }) => {
  const encoder = new Encoder()

  // Messages in a message set are __not__ encoded as an array.
  // They are written in sequence.
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

  entries.forEach(entry => {
    const message = Message(entry)

    // This is the offset used in kafka as the log sequence number.
    // When the producer is sending non compressed messages, it can set the offsets to anything
    encoder.writeInt64(entry.offset || -1)
    encoder.writeInt32(message.size())

    encoder.writeEncoder(message)
  })

  return encoder
}
