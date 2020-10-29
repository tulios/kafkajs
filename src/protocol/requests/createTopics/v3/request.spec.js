const RequestV3Protocol = require('./request')

describe('Protocol > Requests > CreateTopics > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      topics: [
        { topic: 'test-topic-fde67b5a797984ac0837-55492-1bf2f30a-cce8-403d-8897-6902a0b86fb0' },
        { topic: 'test-topic-3d6c53af2e0f9b1d1757-55492-cbde2344-d9d3-4ad7-b408-996cda13e6e5' },
      ],
      validateOnly: false,
      timeout: 5000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request.json')))
  })
})
