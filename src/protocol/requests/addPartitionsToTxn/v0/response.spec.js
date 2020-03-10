const { decode, parse } = require('./response')
const { KafkaJSProtocolError } = require('../../../../errors')

describe('Protocol > Requests > AddPartitionsToTxn > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errors: [
        {
          topic: 'test-topic-f6bab978bdca094688e3-37015-ca4f7ad4-5dcc-4bb9-9853-b1e4c4ed78a7',
          partitionErrors: [
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
      errors: [
        {
          topic: 'test-topic',
          partitionErrors: [
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
