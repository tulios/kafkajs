const Encoder = require('./encoder')

module.exports = ({ correlationId, clientId, request: { apiKey, apiVersion, encode } }) => {
  const payload = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString(clientId)
    .writeEncoder(encode())

  return new Encoder().writeInt32(payload.size()).writeEncoder(payload)
}
