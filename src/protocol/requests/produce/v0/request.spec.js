const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')
const MessageSet = require('../../../messageSet')

describe('Protocol > Requests > Produce > v0', () => {
  let args, messageSet1, messageSet2

  beforeEach(() => {
    messageSet1 = [
      { key: '1', value: 'value-1' },
      { key: '2', value: 'value-2' },
    ]
    messageSet2 = [{ key: '3', value: 'value-3' }]
    args = {
      acks: -1,
      timeout: 1000,
      topicData: [
        {
          topic: 'test-topic-1',
          partitions: [
            { partition: 0, messages: messageSet1 },
            { partition: 1, messages: messageSet2 },
          ],
        },
      ],
    }
  })

  describe('request', () => {
    test('metadata about the API', () => {
      const request = RequestProtocol(args)
      expect(request.apiKey).toEqual(apiKeys.Produce)
      expect(request.apiVersion).toEqual(0)
      expect(request.apiName).toEqual('Produce')
      expect(request.expectResponse()).toEqual(true)
    })

    describe('when acks=0', () => {
      test('expectResponse returns false', () => {
        const request = RequestProtocol({ ...args, acks: 0 })
        expect(request.expectResponse()).toEqual(false)
      })
    })

    test('encode', async () => {
      const request = RequestProtocol(args)
      const ms1 = MessageSet({ entries: args.topicData[0].partitions[0].messages })
      const ms2 = MessageSet({ entries: args.topicData[0].partitions[1].messages })

      const encoder = new Encoder()
        .writeInt16(-1)
        .writeInt32(1000)
        .writeArray([
          new Encoder().writeString('test-topic-1').writeArray([
            new Encoder()
              .writeInt32(0)
              .writeInt32(ms1.size())
              .writeEncoder(ms1),
            new Encoder()
              .writeInt32(1)
              .writeInt32(ms2.size())
              .writeEncoder(ms2),
          ]),
        ])

      const data = await request.encode()
      expect(data.toJSON()).toEqual(encoder.toJSON())
    })
  })
})
