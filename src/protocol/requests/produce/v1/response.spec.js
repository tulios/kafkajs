const Encoder = require('../../../encoder')
const { createErrorFromCode } = require('../../../error')
const response = require('./response')

describe('Protocol > Requests > Produce > v1', () => {
  let decoded

  beforeEach(() => {
    decoded = {
      topics: [
        {
          topicName: 'test-topic-1',
          partitions: [
            // offset is a string to prevent int64 outside of Number.MAX_VALUE to be rounded
            { partition: 0, errorCode: 0, offset: '16' },
            { partition: 1, errorCode: 0, offset: '2' },
          ],
        },
        {
          topicName: 'test-topic-2',
          partitions: [{ partition: 4, errorCode: 0, offset: '11' }],
        },
      ],
      throttleTime: 1000,
    }
  })

  describe('response', () => {
    test('decode', async () => {
      const encoded = new Encoder()
        .writeArray([
          new Encoder().writeString('test-topic-1').writeArray([
            new Encoder()
              .writeInt32(0)
              .writeInt16(0)
              .writeInt64(16),
            new Encoder()
              .writeInt32(1)
              .writeInt16(0)
              .writeInt64(2),
          ]),
          new Encoder().writeString('test-topic-2').writeArray([
            new Encoder()
              .writeInt32(4)
              .writeInt16(0)
              .writeInt64(11),
          ]),
        ])
        .writeInt32(1000)

      const decodedPayload = await response.decode(encoded.buffer)
      expect(decodedPayload).toEqual(decoded)
    })

    test('parse', async () => {
      const parsedPayload = await response.parse(decoded)
      expect(parsedPayload).toEqual(decoded)
    })

    test('when errorCode is different than SUCCESS_CODE', async () => {
      decoded.topics[0].partitions[0].errorCode = 5
      await expect(response.parse(decoded)).rejects.toHaveProperty(
        'message',
        createErrorFromCode(5).message
      )
    })
  })
})
