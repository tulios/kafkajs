const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v6', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v6_response.json')))
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-390850453b1c004039ea-1417-1c32a507-edbb-481d-9d9c-e287743f4b74',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '0',
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
