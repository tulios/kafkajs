const { decode, parse } = require('./response')

describe('Protocol > Requests > SyncGroup > v0', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      errorCode: 0,
      memberAssignment: Buffer.from(
        JSON.stringify({
          'topic-test': [2, 5, 4, 1, 3, 0],
        })
      ),
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
