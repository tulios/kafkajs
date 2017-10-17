const Encoder = require('../encoder')
const Decoder = require('../decoder')
const MessageProtocol = require('../message')
const MessageSet = require('./index')
const MessageSetDecoder = require('./decoder')

describe('Protocol > MessageSet > decoder', () => {
  describe('v0', () => {
    test('decode', () => {
      const messageSet = MessageSet({
        messageVersion: 0,
        entries: [{ key: 'v0-key', value: 'v0-value', offset: 10 }],
      })

      const { buffer } = new Encoder().writeInt32(messageSet.size()).writeEncoder(messageSet)
      const decoder = new Decoder(buffer)

      expect(MessageSetDecoder(decoder)).toEqual([
        {
          offset: '10',
          size: 28,
          crc: 1857563124,
          magicByte: 0,
          attributes: 0,
          key: Buffer.from('v0-key'),
          value: Buffer.from('v0-value'),
        },
      ])
    })

    test('skip partial messages', () => {
      const messageSet = MessageSet({
        messageVersion: 0,
        entries: [{ key: 'v0-key', value: 'v0-value', offset: 10 }],
      })

      // read some bytes to simulate a partial message
      const messageSetBuffer = messageSet.buffer.slice(
        Decoder.int32Size(),
        messageSet.buffer.length
      )

      const temp = new Encoder()
      temp.buffer = messageSetBuffer

      const { buffer } = new Encoder().writeInt32(messageSet.size()).writeEncoder(temp)
      const decoder = new Decoder(buffer)

      expect(MessageSetDecoder(decoder)).toEqual([])
    })
  })
})
