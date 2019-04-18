const { decode, parse } = require('./response')

describe('Protocol > Requests > LeaveGroup > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({ throttleTime: 0, errorCode: 0 })
    await expect(parse(data)).resolves.toBeTruthy()
  })
})
