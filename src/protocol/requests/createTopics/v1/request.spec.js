const RequestV1Protocol = require('./request')

describe('Protocol > Requests > CreateTopics > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      topics: [
        { topic: 'test-topic-c8d8ca3d95495c6b900d' },
        { topic: 'test-topic-050fb2e6aed13a954288' },
      ],
      timeout: 5000,
      validateOnly: true,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
