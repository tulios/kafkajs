const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetFetch > v4', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      responses: [
        {
          topic: 'test-topic-df48241c4bf2fca9d16b-20117-aff9b64c-69a2-4456-be7b-de5bcd78984e',
          partitions: [{ partition: 0, offset: '-1', metadata: '', errorCode: 0 }],
        },
      ],
      errorCode: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
