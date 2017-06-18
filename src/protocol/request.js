const Encoder = require('./encoder')

module.exports = ({ correlationId, clientId, message: { apiKey, apiVersion, protocol } }) => {
  const request = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString(clientId)
    .writeEncoder(protocol())

  return new Encoder().writeInt32(request.size()).writeEncoder(request)
}
