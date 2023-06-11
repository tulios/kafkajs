const offsetDeleteRequest = require('./request')

describe('Protocol > Requests > OffsetDelete > v0', () => {
  test('request', async () => {
    const { apiKey, apiVersion, apiName, encode } = offsetDeleteRequest({
      groupId: 'foo',
      topics: [
        {
          topic: 'bar',
          partitions: [0, 1, 2, 3],
        },
      ],
    })
    const { buffer } = await encode()

    expect(apiKey).toEqual(47)
    expect(apiVersion).toEqual(0)
    expect(apiName).toEqual('OffsetDelete')
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
