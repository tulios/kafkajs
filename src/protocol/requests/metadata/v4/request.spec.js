const Encoder = require('../../../encoder')
const RequestProtocol = require('./request')

describe('Protocol > Requests > Metadata > v4', () => {
  let topics

  beforeEach(() => {
    topics = ['test-topic-1', 'test-topic-2']
  })

  test('request', async () => {
    const request = RequestProtocol({ topics, allowAutoTopicCreation: true })
    const encoder = new Encoder().writeArray(topics).writeBoolean(true)
    const data = await request.encode()
    expect(data.toJSON()).toEqual(encoder.toJSON())
  })
})
