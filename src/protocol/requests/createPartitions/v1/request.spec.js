const apiKeys = require('../../apiKeys')
const RequestV1Protocol = require('./request')

describe('Protocol > Requests > CreatePartitions > v1', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV1Protocol({})
      expect(request.apiKey).toEqual(apiKeys.CreatePartitions)
      expect(request.apiVersion).toEqual(1)
      expect(request.apiName).toEqual('CreatePartitions')
    })

    test('encode', async () => {
      const { buffer } = await RequestV1Protocol({
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
      expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
    })
  })
})
