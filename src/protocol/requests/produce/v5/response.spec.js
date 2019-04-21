const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v5', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response.json')))
    expect(data).toEqual({
      topics: [
        {
          partitions: [
            {
              baseOffset: '0',
              errorCode: 0,
              logAppendTime: '-1',
              logStartOffset: '0',
              partition: 0,
            },
          ],
          topicName: 'test-topic-1c8ace0ecfb3cb281243-706-b9f24ac1-6a1e-4458-ba5f-5fc0c51a46c7',
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
