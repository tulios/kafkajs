const { decode, parse } = require('./response')
const { KafkaJSProtocolError } = require('../../../../errors')

describe('Protocol > Requests > TxnOffsetCommit > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic-0ba33173f7664d75c6b2-63632-a0dab079-1c9a-44ba-be25-ca3d50df5003',
          partitions: [
            { errorCode: 0, partition: 1 },
            { errorCode: 0, partition: 2 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if there is an error on any of the partitions', async () => {
    const data = {
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            { errorCode: 0, partition: 1 },
            { errorCode: 49, partition: 2 },
          ],
        },
      ],
    }

    await expect(parse(data)).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })
})
