const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')

describe('Protocol > Requests > ApiVersions > v0', () => {
  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestProtocol()
      expect(request.apiKey).toEqual(apiKeys.ApiVersions)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('ApiVersions')
    })

    test('encode', () => {
      const request = RequestProtocol()
      expect(request.encode().toJSON()).toEqual(new Encoder().toJSON())
    })
  })
})
