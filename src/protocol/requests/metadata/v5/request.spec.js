const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')

describe('Protocol > Requests > Metadata > v5', async () => {
  let topics

  beforeEach(() => {
    topics = ['test-topic-1', 'test-topic-2']
  })

  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestProtocol({ topics })
      expect(request.apiKey).toEqual(apiKeys.Metadata)
      expect(request.apiVersion).toEqual(5)
      expect(request.apiName).toEqual('Metadata')
    })

    test('encode', async () => {
      const request = RequestProtocol({ topics, allowAutoTopicCreation: true })
      const encoder = new Encoder().writeArray(topics).writeBoolean(true)
      const data = await request.encode()
      expect(data.toJSON()).toEqual(encoder.toJSON())
    })
  })
})
