const apiKeys = require('../../apiKeys')
const RequestV0Protocol = require('./request')

describe('Protocol > Requests > DeleteRecords > v0', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV0Protocol({})
      expect(request.apiKey).toEqual(apiKeys.DeleteRecords)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('DeleteRecords')
    })

    test('encode', async () => {
      const { buffer } = await RequestV0Protocol({
        topics: [
          {
            topic: 'test-topic-42132ca1c79e5dd6c436-81884-14d3a181-013d-4176-8e7e-7518a67f4813',
            partitions: [
              {
                partition: 0,
                offset: '7',
              },
            ],
          },
        ],
        timeout: 5000,
      }).encode()
      expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
    })
  })
})
