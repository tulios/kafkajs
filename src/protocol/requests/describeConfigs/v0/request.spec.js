const apiKeys = require('../../apiKeys')
const RequestV0Protocol = require('./request')
const RESOURCE_TYPES = require('../../../resourceTypes')

describe('Protocol > Requests > DescribeConfigs > v0', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV0Protocol({})
      expect(request.apiKey).toEqual(apiKeys.DescribeConfigs)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('DescribeConfigs')
    })

    test('encode', async () => {
      const { buffer } = await RequestV0Protocol({
        resources: [
          {
            type: RESOURCE_TYPES.TOPIC,
            name: 'test-topic-332d38bc4eee2ff29df6',
            configNames: ['compression.type', 'retention.ms'],
          },
        ],
      }).encode()
      expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
    })
  })
})
