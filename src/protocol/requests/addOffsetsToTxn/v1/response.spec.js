const { decode, parse } = require('./response')
const { unsupportedVersionResponseWithTimeout } = require('testHelpers')

describe('Protocol > Requests > AddPartitionsToTxn > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponseWithTimeout())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
