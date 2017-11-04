const { decode, parse } = require('../v0/response')

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
})
