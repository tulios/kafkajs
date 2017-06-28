const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const { SUCCESS_CODE, KafkaProtocolError } = require('../../../error')
const response = require('./response')

describe('Protocol > Requests > Metadata > v1', () => {
  let decoded

  beforeEach(() => {
    decoded = {
      brokers: [{ nodeId: 0, host: 'localhost', port: 9092, rack: 'something' }],
      controllerId: 3,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-1',
          isInternal: true,
          partitionMetadata: [
            { partitionErrorCode: 0, partitionId: 1, leader: 2, replicas: [3], isr: [4] },
          ],
        },
      ],
    }
  })

  describe('response', () => {
    test('decode', () => {
      const encoded = new Encoder()
        .writeArray([
          new Encoder()
            .writeInt32(0)
            .writeString('localhost')
            .writeInt32(9092)
            .writeString('something'),
        ])
        .writeInt32(3)
        .writeArray([
          new Encoder()
            .writeInt16(0)
            .writeString('test-topic-1')
            .writeBoolean(true)
            .writeArray([
              new Encoder()
                .writeInt16(0)
                .writeInt32(1)
                .writeInt32(2)
                .writeArray([3], 'int32')
                .writeArray([4], 'int32'),
            ]),
        ])

      expect(response.decode(encoded.buffer)).toEqual(decoded)
    })

    test('parse', () => {
      expect(response.parse(decoded)).toEqual(decoded)
    })

    test('when topicErrorCode is different than SUCCESS_CODE', () => {
      decoded.topicMetadata[0].topicErrorCode = 5
      expect(() => response.parse(decoded)).toThrowError(new KafkaProtocolError(5))
    })

    test('when partitionErrorCode is different than SUCCESS_CODE', () => {
      decoded.topicMetadata[0].partitionMetadata[0].partitionErrorCode = 5
      expect(() => response.parse(decoded)).toThrowError(new KafkaProtocolError(5))
    })
  })
})
