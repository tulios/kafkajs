const { decode, parse } = require('./response')

describe('Protocol > Requests > CreateAcls > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      creationResponses: [{ errorCode: 0, errorMessage: null }],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
