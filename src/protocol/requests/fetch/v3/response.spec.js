const { decode, parse } = require('../v1/response')

describe('Protocol > Requests > Fetch > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-131c279f35eeb2df6bc7',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '3',
              messages: [
                {
                  offset: '0',
                  size: 39,
                  crc: -815808405,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715172',
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                },
                {
                  offset: '1',
                  size: 39,
                  crc: -656171735,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715173',
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                },
                {
                  offset: '2',
                  size: 39,
                  crc: 309368599,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715173',
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

  test('response with GZIP', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response_gzip.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-7d48c01ed48f0667672b',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '3',
              messages: [
                {
                  offset: '0',
                  size: 39,
                  crc: -822662985,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715533',
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                },
                {
                  offset: '1',
                  size: 39,
                  crc: -872333670,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715534',
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                },
                {
                  offset: '2',
                  size: 39,
                  crc: 110245028,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1509827715534',
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
