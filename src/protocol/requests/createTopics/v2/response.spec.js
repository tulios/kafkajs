const { decode, parse } = require('./response')

describe('Protocol > Requests > CreateTopics > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      topicErrors: [
        {
          topic: 'test-topic-3d6c53af2e0f9b1d1757-55492-cbde2344-d9d3-4ad7-b408-996cda13e6e5',
          errorCode: 0,
          errorMessage: null,
        },
        {
          errorCode: 0,
          errorMessage: null,
          topic: 'test-topic-fde67b5a797984ac0837-55492-1bf2f30a-cce8-403d-8897-6902a0b86fb0',
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
