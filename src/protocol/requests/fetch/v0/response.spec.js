const { decode, parse } = require('./response')

describe('Protocol > Requests > Fetch > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      responses: [
        {
          topicName: 'test-topic-79b94d9dcfd65e1283a9',
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
    const data = await decode(Buffer.from(require('../fixtures/v0_response_gzip.json')))
    expect(data).toEqual({
      responses: [
        {
          topicName: 'test-topic-be3cb8c367c9d903933f',
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

  test('throw KafkaJSOffsetOutOfRange error when the requested offset is not within the range of offsets maintained by the server', async () => {
    const data = {
      responses: [
        {
          topicName: 'test-topic-79b94d9dcfd65e1283a9',
          partitions: [
            {
              errorCode: 1,
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
    }

    await expect(parse(data)).rejects.toHaveProperty('name', 'KafkaJSOffsetOutOfRange')
  })

  test('throw KafkaJSProtocolError for all other errors', async () => {
    const data = {
      responses: [
        {
          topicName: 'test-topic-79b94d9dcfd65e1283a9',
          partitions: [
            {
              errorCode: 2,
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
    }

    await expect(parse(data)).rejects.toHaveProperty('name', 'KafkaJSProtocolError')
  })
})
