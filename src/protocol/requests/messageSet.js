const Encoder = require('../encoder')
const messageProtocol = require('./message')

/**
* MessageSet => [Offset MessageSize Message]
*   Offset => int64
*   MessageSize => int32
*/
module.exports = ({ offset, messageSize, messages }) => {
  const encoder = new Encoder()
  messages.forEach((message, i) => {
    encoder.writeEncoder(
      messageProtocol(
        Object.assign(message, {
          offset: i,
        })
      )
    )
  })
  return encoder
}
