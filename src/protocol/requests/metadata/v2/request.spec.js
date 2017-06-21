const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')

describe('Protocol > Requests > Metadata > v2', () => {
  let topics

  beforeEach(() => {
    topics = ['test-topic-1', 'test-topic-2']
  })

  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestProtocol(topics)
      expect(request.apiKey).toEqual(apiKeys.Metadata)
      expect(request.apiVersion).toEqual(2)
      expect(request.apiName).toEqual('Metadata')
    })

    test('encode', () => {
      const request = RequestProtocol(topics)
      const encoder = new Encoder().writeArray(topics)
      expect(request.encode().toJSON()).toEqual(encoder.toJSON())
    })
  })
})
