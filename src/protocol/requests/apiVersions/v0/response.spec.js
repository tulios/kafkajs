const { unsupportedVersionResponse } = require('testHelpers')
const { decode, parse } = require('./response')

describe('Protocol > Requests > ApiVersions > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      apiVersions: [
        { apiKey: 0, minVersion: 0, maxVersion: 2 },
        { apiKey: 1, minVersion: 0, maxVersion: 3 },
        { apiKey: 2, minVersion: 0, maxVersion: 1 },
        { apiKey: 3, minVersion: 0, maxVersion: 2 },
        { apiKey: 4, minVersion: 0, maxVersion: 0 },
        { apiKey: 5, minVersion: 0, maxVersion: 0 },
        { apiKey: 6, minVersion: 0, maxVersion: 3 },
        { apiKey: 7, minVersion: 1, maxVersion: 1 },
        { apiKey: 8, minVersion: 0, maxVersion: 2 },
        { apiKey: 9, minVersion: 0, maxVersion: 2 },
        { apiKey: 10, minVersion: 0, maxVersion: 0 },
        { apiKey: 11, minVersion: 0, maxVersion: 1 },
        { apiKey: 12, minVersion: 0, maxVersion: 0 },
        { apiKey: 13, minVersion: 0, maxVersion: 0 },
        { apiKey: 14, minVersion: 0, maxVersion: 0 },
        { apiKey: 15, minVersion: 0, maxVersion: 0 },
        { apiKey: 16, minVersion: 0, maxVersion: 0 },
        { apiKey: 17, minVersion: 0, maxVersion: 0 },
        { apiKey: 18, minVersion: 0, maxVersion: 0 },
        { apiKey: 19, minVersion: 0, maxVersion: 1 },
        { apiKey: 20, minVersion: 0, maxVersion: 0 },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponse())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
