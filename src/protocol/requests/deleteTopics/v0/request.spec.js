const RequestV0Protocol = require('./request')

describe('Protocol > Requests > DeleteTopics > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: ['test-topic-5f80283ca8a1e46d2273', 'test-topic-34631544b8db1d1263b9'],
      timeout: 5000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
