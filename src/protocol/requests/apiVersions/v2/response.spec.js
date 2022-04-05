const { unsupportedVersionResponse } = require('testHelpers')
const { decode, parse } = require('./response')

describe('Protocol > Requests > ApiVersions > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      apiVersions: [
        { apiKey: 0, maxVersion: 3, minVersion: 0 },
        { apiKey: 1, maxVersion: 5, minVersion: 0 },
        { apiKey: 2, maxVersion: 2, minVersion: 0 },
        { apiKey: 3, maxVersion: 4, minVersion: 0 },
        { apiKey: 4, maxVersion: 0, minVersion: 0 },
        { apiKey: 5, maxVersion: 0, minVersion: 0 },
        { apiKey: 6, maxVersion: 3, minVersion: 0 },
        { apiKey: 7, maxVersion: 1, minVersion: 1 },
        { apiKey: 8, maxVersion: 3, minVersion: 0 },
        { apiKey: 9, maxVersion: 3, minVersion: 0 },
        { apiKey: 10, maxVersion: 1, minVersion: 0 },
        { apiKey: 11, maxVersion: 2, minVersion: 0 },
        { apiKey: 12, maxVersion: 1, minVersion: 0 },
        { apiKey: 13, maxVersion: 1, minVersion: 0 },
        { apiKey: 14, maxVersion: 1, minVersion: 0 },
        { apiKey: 15, maxVersion: 1, minVersion: 0 },
        { apiKey: 16, maxVersion: 1, minVersion: 0 },
        { apiKey: 17, maxVersion: 0, minVersion: 0 },
        { apiKey: 18, maxVersion: 1, minVersion: 0 },
        { apiKey: 19, maxVersion: 2, minVersion: 0 },
        { apiKey: 20, maxVersion: 1, minVersion: 0 },
        { apiKey: 21, maxVersion: 0, minVersion: 0 },
        { apiKey: 22, maxVersion: 0, minVersion: 0 },
        { apiKey: 23, maxVersion: 0, minVersion: 0 },
        { apiKey: 24, maxVersion: 0, minVersion: 0 },
        { apiKey: 25, maxVersion: 0, minVersion: 0 },
        { apiKey: 26, maxVersion: 0, minVersion: 0 },
        { apiKey: 27, maxVersion: 0, minVersion: 0 },
        { apiKey: 28, maxVersion: 0, minVersion: 0 },
        { apiKey: 29, maxVersion: 0, minVersion: 0 },
        { apiKey: 30, maxVersion: 0, minVersion: 0 },
        { apiKey: 31, maxVersion: 0, minVersion: 0 },
        { apiKey: 32, maxVersion: 0, minVersion: 0 },
        { apiKey: 33, maxVersion: 0, minVersion: 0 },
      ],
      errorCode: 0,
      clientSideThrottleTime: 0,
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  // https://github.com/tulios/kafkajs/issues/491
  test('defaults throttle_time_ms if it is not provided in the response', async () => {
    const data = await decode(
      Buffer.from(require('../fixtures/v1_response_missing_throttle_time.json'))
    )
    expect(data).toEqual({
      apiVersions: [
        { apiKey: 0, maxVersion: 3, minVersion: 0 },
        { apiKey: 1, maxVersion: 5, minVersion: 0 },
        { apiKey: 2, maxVersion: 2, minVersion: 0 },
        { apiKey: 3, maxVersion: 4, minVersion: 0 },
        { apiKey: 4, maxVersion: 0, minVersion: 0 },
        { apiKey: 5, maxVersion: 0, minVersion: 0 },
        { apiKey: 6, maxVersion: 3, minVersion: 0 },
        { apiKey: 7, maxVersion: 1, minVersion: 1 },
        { apiKey: 8, maxVersion: 3, minVersion: 0 },
        { apiKey: 9, maxVersion: 3, minVersion: 0 },
        { apiKey: 10, maxVersion: 1, minVersion: 0 },
        { apiKey: 11, maxVersion: 2, minVersion: 0 },
        { apiKey: 12, maxVersion: 1, minVersion: 0 },
        { apiKey: 13, maxVersion: 1, minVersion: 0 },
        { apiKey: 14, maxVersion: 1, minVersion: 0 },
        { apiKey: 15, maxVersion: 1, minVersion: 0 },
        { apiKey: 16, maxVersion: 1, minVersion: 0 },
        { apiKey: 17, maxVersion: 0, minVersion: 0 },
        { apiKey: 18, maxVersion: 1, minVersion: 0 },
        { apiKey: 19, maxVersion: 2, minVersion: 0 },
        { apiKey: 20, maxVersion: 1, minVersion: 0 },
        { apiKey: 21, maxVersion: 0, minVersion: 0 },
        { apiKey: 22, maxVersion: 0, minVersion: 0 },
        { apiKey: 23, maxVersion: 0, minVersion: 0 },
        { apiKey: 24, maxVersion: 0, minVersion: 0 },
        { apiKey: 25, maxVersion: 0, minVersion: 0 },
        { apiKey: 26, maxVersion: 0, minVersion: 0 },
        { apiKey: 27, maxVersion: 0, minVersion: 0 },
        { apiKey: 28, maxVersion: 0, minVersion: 0 },
        { apiKey: 29, maxVersion: 0, minVersion: 0 },
        { apiKey: 30, maxVersion: 0, minVersion: 0 },
        { apiKey: 31, maxVersion: 0, minVersion: 0 },
        { apiKey: 32, maxVersion: 0, minVersion: 0 },
        { apiKey: 33, maxVersion: 0, minVersion: 0 },
      ],
      errorCode: 0,
      clientSideThrottleTime: 0,
      throttleTime: 0,
    })
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponse())).rejects.toThrow(
      /The version of API is not supported/
    )
  })
})
