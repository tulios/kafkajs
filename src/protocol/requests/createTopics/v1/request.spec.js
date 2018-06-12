const apiKeys = require('../../apiKeys')
const RequestV1Protocol = require('./request')

describe('Protocol > Requests > CreateTopics > v1', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV1Protocol({})
      expect(request.apiKey).toEqual(apiKeys.CreateTopics)
      expect(request.apiVersion).toEqual(1)
      expect(request.apiName).toEqual('CreateTopics')
    })

    test('encode', async () => {
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
})
