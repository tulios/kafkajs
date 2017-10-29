const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetCommit > v0', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      responses: [
        { topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, errorCode: 0 }] },
      ],
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
