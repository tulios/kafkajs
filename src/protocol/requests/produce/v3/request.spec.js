const RequestV3Protocol = require('./request')

describe('Protocol > Requests > Produce > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1526939522195,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    expect(buffer).toEqual('<write test>')
  })
})
