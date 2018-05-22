const RequestV3Protocol = require('./request')

describe('Protocol > Requests > Produce > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      transactionalId: null,
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-b9e9ddb9061e327b2d80',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-5d7b7f5e1099d6cebc05',
                  value: 'some-value-a68c69a39c2183f75e68',
                  timestamp: 1509928155660,
                },
                {
                  key: 'key-f39f8d5ad6fa246973da',
                  value: 'some-value-474805f12b94db8eaecc',
                  timestamp: 1509928155661,
                },
                {
                  key: 'key-cbe31a6b3145c40c29b1',
                  value: 'some-value-914542c511ada04e118c',
                  timestamp: 1509928155662,
                },
              ],
            },
          ],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })
})
