const RequestV2Protocol = require('./request')
const { Types } = require('../../../message/compression')

describe('Protocol > Requests > Produce > v2', () => {
  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
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
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-8a14e73a88e93f7c3a39',
                  value: 'some-value-4fa91513bffbcc0e34b3',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-183a2d8eb3683f080b82',
                  value: 'some-value-938afcf1f2ef0439c752',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request.json')))
  })

  test('request with gzip', async () => {
    const originalDateNow = Date.now
    Date.now = jest.fn(() => 1509819296569)
    const { buffer } = await RequestV2Protocol({
      acks: -1,
      timeout: 30000,
      compression: Types.GZIP,
      topicData: [
        {
          topic: 'test-topic-bc674c30572e8ded886a',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-95600f2c2703a2327b82',
                  value: 'some-value-2220ab161ff7a1d95e81',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-69bc917adcaa022c7b18',
                  value: 'some-value-ee5a8e3f596a2e579c82',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-d5e5057dac669e65bc64',
                  value: 'some-value-02e4e3875d5dcd86a3f3',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    Date.now = originalDateNow
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request_gzip.json')))
  })
})
