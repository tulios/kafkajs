const apiKeys = require('../../apiKeys')
const RequestV0Protocol = require('./request')

describe('Protocol > Requests > ApiVersions > v2', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestV0Protocol()
      expect(request.apiKey).toEqual(apiKeys.ApiVersions)
      expect(request.apiVersion).toEqual(2)
      expect(request.apiName).toEqual('ApiVersions')
    })

    test('encode', async () => {
      const { buffer } = await RequestV0Protocol().encode()
      expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
    })
  })
})
