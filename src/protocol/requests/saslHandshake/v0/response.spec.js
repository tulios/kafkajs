const { unsupportedVersionResponse } = require('testHelpers')
const { decode } = require('./response')

describe('Protocol > Requests > SASLHandshake > v0', () => {
  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponse())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
