const { decode, parse } = require('../v1/response')

describe('Protocol > Requests > Fetch > v1', () => {
  test('response', () => {
    const data = decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-6354595aa07c0fa2ae55',
          partitions: [
            {
              errorCode: 0,
              highWatermark: '1',
              partition: 0,
              messages: [
                {
                  attributes: 0,
                  crc: 120234579,
                  magicByte: 0,
                  offset: '0',
                  size: 31,
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                },
              ],
            },
          ],
        },
      ],
    })

    expect(() => parse(data)).not.toThrowError()
  })
})
