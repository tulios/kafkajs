const { decode, parse } = require('./response')

describe('Protocol > Requests > CreateAcls > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      creationResponses: [{ errorCode: 0, errorMessage: null }],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
