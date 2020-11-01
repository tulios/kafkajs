const { decode, parse } = require('./response')

describe('Protocol > Requests > ListOffsets > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      responses: [
        {
          topic: 'test-topic-84efe7aaafc3844b00c1-36211-2ee431b4-d40b-4df8-b2c8-fc9e33ab5c77',
          partitions: [{ partition: 0, errorCode: 0, timestamp: '-1', offset: '1' }],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
