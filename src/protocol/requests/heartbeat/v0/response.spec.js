const { decode, parse } = require('./response')

describe('Protocol > Requests > Heartbeat > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({ errorCode: 0 })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
