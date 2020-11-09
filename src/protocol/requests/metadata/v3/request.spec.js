const Encoder = require('../../../encoder')
const RequestProtocol = require('./request')

describe('Protocol > Requests > Metadata > v3', () => {
  let topics

  beforeEach(() => {
    topics = ['test-topic-1', 'test-topic-2']
  })

  test('request', async () => {
    const request = RequestProtocol({ topics })
    const encoder = new Encoder().writeArray(topics)
    const data = await request.encode()
    expect(data.toJSON()).toEqual(encoder.toJSON())
  })
})
