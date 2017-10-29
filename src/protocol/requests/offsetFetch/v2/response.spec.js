const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetFetch > v2', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-2cbbd6e6362f1a638c94',
          partitions: [{ partition: 0, offset: '-1', metadata: '', errorCode: 0 }],
        },
      ],
      errorCode: 0,
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
