const RequestV0Protocol = require('./request')

describe('Protocol > Requests > DeleteRecords > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'test-topic-42132ca1c79e5dd6c436-81884-14d3a181-013d-4176-8e7e-7518a67f4813',
          partitions: [
            {
              partition: 0,
              offset: '7',
            },
          ],
        },
      ],
      timeout: 5000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
