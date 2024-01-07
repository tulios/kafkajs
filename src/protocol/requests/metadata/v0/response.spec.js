const Encoder = require('../../../encoder')
const { createErrorFromCode } = require('../../../error')
const response = require('./response')

describe('Protocol > Requests > Metadata > v0', () => {
  let decoded

  beforeEach(() => {
    decoded = {
      brokers: [{ nodeId: 0, host: 'localhost', port: 9092 }],
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-1',
          partitionMetadata: [
            { partitionErrorCode: 0, partitionId: 1, leader: 2, replicas: [3], isr: [4] },
          ],
        },
      ],
    }
  })

  describe('response', () => {
    test('decode', async () => {
      const encoded = new Encoder()
        .writeArray([
          new Encoder()
            .writeInt32(0)
            .writeString('localhost')
            .writeInt32(9092),
        ])
        .writeArray([
          new Encoder()
            .writeInt16(0)
            .writeString('test-topic-1')
            .writeArray([
              new Encoder()
                .writeInt16(0)
                .writeInt32(1)
                .writeInt32(2)
                .writeArray([3], 'int32')
                .writeArray([4], 'int32'),
            ]),
        ])

      const decodedPayload = await response.decode(encoded.buffer)
      expect(decodedPayload).toEqual(decoded)
    })

    test('parse', async () => {
      const parsedPayload = await response.parse(decoded)
      expect(parsedPayload).toEqual(decoded)
    })

    test('when topicErrorCode is different than SUCCESS_CODE', async () => {
      decoded.topicMetadata[0].topicErrorCode = 5
      await expect(response.parse(decoded)).rejects.toHaveProperty(
        'message',
        createErrorFromCode(5).message
      )
    })

    test('when partitionErrorCode is different than SUCCESS_CODE', async () => {
      decoded.topicMetadata[0].partitionMetadata[0].partitionErrorCode = 5
      await expect(response.parse(decoded)).rejects.toHaveProperty(
        'message',
        createErrorFromCode(5).message
      )
    })

    test('when topicErrorCode is UNKNOWN_TOPIC_OR_PARTITION', async () => {
      decoded.topicMetadata[0].topicErrorCode = 3
      await expect(response.parse(decoded)).rejects.toMatchObject({
        message: createErrorFromCode(3).message + ' [test-topic-1]',
        retriable: false,
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
        name: 'KafkaJSUnknownTopic',
        topic: 'test-topic-1',
      })
    })

    test('when topicErrorCode is TOPIC_AUTHORIZATION_FAILED', async () => {
      decoded.topicMetadata[0].topicErrorCode = 29
      await expect(response.parse(decoded)).rejects.toMatchObject({
        message: createErrorFromCode(29).message + ' [test-topic-1]',
        retriable: false,
        type: 'TOPIC_AUTHORIZATION_FAILED',
        code: 29,
        name: 'KafkaJSTopicAuthorizationFailed',
        topic: 'test-topic-1',
      })
    })
  })
})
