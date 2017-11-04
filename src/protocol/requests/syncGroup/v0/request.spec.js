const RequestV0Protocol = require('./request')

describe('Protocol > Requests > SyncGroup > v0', () => {
  test('request', async () => {
    const groupId = 'test-group'
    const memberId = 'example-consumer-a7384422-58ff-476d-bb01-7e4ecc376578'

    const { buffer } = await RequestV0Protocol({
      groupId,
      generationId: 1,
      memberId,
      groupAssignment: [
        {
          memberId,
          memberAssignment: {
            'topic-test': [2, 5, 4, 1, 3, 0],
          },
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
