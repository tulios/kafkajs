const apiKeys = require('../../apiKeys')
const RequestV2Protocol = require('./request')

describe('Protocol > Requests > CreateTopics > v2', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV2Protocol({})
      expect(request.apiKey).toEqual(apiKeys.CreateTopics)
      expect(request.apiVersion).toEqual(2)
      expect(request.apiName).toEqual('CreateTopics')
    })

    test('encode', async () => {
      const { buffer } = await RequestV2Protocol({
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
})
