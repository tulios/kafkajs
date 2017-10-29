const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetFetch > v0', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-9f9b074057acd4335946',
          partitions: [{ partition: 0, offset: '-1', metadata: '', errorCode: 0 }],
        },
      ],
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
