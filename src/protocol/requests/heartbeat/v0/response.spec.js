const { decode, parse } = require('./response')

describe('Protocol > Requests > Heartbeat > v0', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({ errorCode: 0 })

    expect(() => parse(data)).not.toThrowError()
  })
})
