const { decode, parse } = require('../v1/response')

describe('Protocol > Requests > Fetch > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
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

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('response with GZIP', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response_gzip.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-ae0b74cd45cb7c1971dd',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '3',
              messages: [
                {
                  offset: '0',
                  size: 31,
                  crc: 120234579,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                },
                {
                  offset: '1',
                  size: 31,
                  crc: -141862522,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                },
                {
                  offset: '2',
                  size: 31,
                  crc: 1025004472,
                  magicByte: 0,
                  attributes: 0,
                  key: Buffer.from('key-2'),
                  value: Buffer.from('some-value-2'),
                },
              ],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
