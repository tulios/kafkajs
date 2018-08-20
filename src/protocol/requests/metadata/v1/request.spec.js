const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')

describe('Protocol > Requests > Metadata > v1', () => {
  let topics

  beforeEach(() => {
    topics = ['test-topic-1', 'test-topic-2']
  })

  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestProtocol({ topics })
      expect(request.apiKey).toEqual(apiKeys.Metadata)
      expect(request.apiVersion).toEqual(1)
      expect(request.apiName).toEqual('Metadata')
    })

    test('encode', async () => {
      const request = RequestProtocol({ topics })
      const encoder = new Encoder().writeArray(topics)
      const data = await request.encode()
      expect(data.toJSON()).toEqual(encoder.toJSON())
    })
  })
})
