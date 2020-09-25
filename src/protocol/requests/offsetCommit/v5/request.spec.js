const RequestV4Protocol = require('./request')

describe('Protocol > Requests > OffsetCommit > v5', () => {
  test('request', async () => {
    const { buffer } = await RequestV4Protocol({
      groupId: 'consumer-group-id-ca28067439d6194a9625-9985-cbb81a97-5151-4658-a055-c479147b107d',
      groupGenerationId: 1,
      memberId:
        'test-f5e359ffa7b2578aca4b-9985-60dcd0da-1130-4eaa-99aa-9bd80f39eceb-4426ce19-8149-4d64-b8c3-841d3bb7ca26',
      topics: [
        {
          topic: 'test-topic-5c24efe0ac41b91bee85-9985-841d6145-c897-4471-bd09-acd8b4c905f2',
          partitions: [{ partition: 0, offset: '0' }],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request.json')))
  })

  test('request with metadata', async () => {
    const { buffer } = await RequestV4Protocol({
      groupId: 'consumer-group-id-ca28067439d6194a9625-9985-cbb81a97-5151-4658-a055-c479147b107d',
      groupGenerationId: 1,
      memberId:
        'test-f5e359ffa7b2578aca4b-9985-60dcd0da-1130-4eaa-99aa-9bd80f39eceb-4426ce19-8149-4d64-b8c3-841d3bb7ca26',
      topics: [
        {
          topic: 'test-topic-5c24efe0ac41b91bee85-9985-841d6145-c897-4471-bd09-acd8b4c905f2',
          partitions: [{ partition: 0, offset: '0', metadata: 'test' }],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request_metadata.json')))
  })
})
