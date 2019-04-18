const RequestV1Protocol = require('./request')

describe('Protocol > Requests > SyncGroup > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      groupId: 'consumer-group-id-e15bd537f491e89484f1-24495-5c083268-3a66-4366-8afc-2c429edeb6af',
      generationId: 1,
      memberId:
        'test-d44f97e7d1a0622387a1-24495-d057a55d-fb7c-446d-98b7-3a3a8dff7944-1f460f6f-bf82-4448-9c18-09b0d7eaceb6',
      groupAssignment: [
        {
          memberId:
            'test-d44f97e7d1a0622387a1-24495-d057a55d-fb7c-446d-98b7-3a3a8dff7944-1f460f6f-bf82-4448-9c18-09b0d7eaceb6',
          memberAssignment: Buffer.from(require('../fixtures/v1_memberAssignment')),
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
