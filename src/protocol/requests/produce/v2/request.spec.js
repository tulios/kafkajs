const Encoder = require('../../../encoder')
const apiKeys = require('../../apiKeys')
const RequestProtocol = require('./request')
const MessageSet = require('../../../messageSet')

describe('Protocol > Requests > Produce > v2', () => {
  let args, messageSet1, messageSet2

  beforeEach(() => {
    messageSet1 = [
      { key: '1', value: 'value-1', timestamp: 1498074767737 },
      { key: '2', value: 'value-2', timestamp: 1498074767738 },
    ]
    messageSet2 = [{ key: '3', value: 'value-3', timestamp: 1498074767739 }]
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
      expect(request.apiVersion).toEqual(2)
      expect(request.apiName).toEqual('Produce')
    })

    test('encode', async () => {
      const request = RequestProtocol(args)
      const ms1 = MessageSet({
        messageVersion: 1,
        entries: args.topicData[0].partitions[0].messages,
      })
      const ms2 = MessageSet({
        messageVersion: 1,
        entries: args.topicData[0].partitions[1].messages,
      })

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
