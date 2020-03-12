const apiKeys = require('../../apiKeys')
const RequestV0Protocol = require('./request')

describe('Protocol > Requests > CreatePartitions > v0', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV0Protocol({})
      expect(request.apiKey).toEqual(apiKeys.CreatePartitions)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('CreatePartitions')
    })

    test('encode', async () => {
      const { buffer } = await RequestV0Protocol({
        topicPartitions: [
          {
            topic: 'test-topic-c8d8ca3d95495c6b900d',
            count: 3,
          },
          {
            topic: 'test-topic-050fb2e6aed13a954288',
            count: 5,
            assignments: [[0], [1], [2]],
          },
        ],
        timeout: 5000,
      }).encode()
      expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
    })
  })
})
