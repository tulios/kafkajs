const { decode, parse } = require('./response')

describe('Protocol > Requests > FindCoordinator > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 2, host: '192.168.50.211', port: 9098 },
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(
      decode(Buffer.from(require('../fixtures/v1_response_version_error.json')))
    ).rejects.toThrow(/The version of API is not supported/)
  })
})
