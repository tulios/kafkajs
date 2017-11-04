const RequestV0Protocol = require('./request')

describe('Protocol > Requests > Offsets > v0', () => {
  test('request', async () => {
    const timestamp = 1509285569484
    const topics = [
      {
        topic: 'test-topic-727705ce68c29fedddf4',
        partitions: [{ partition: 0, timestamp }],
      },
    ]

    const { buffer } = await RequestV0Protocol({ replicaId: -1, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
