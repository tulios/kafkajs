const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v7', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v7_response.json')))
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-923030b997a626c23158-517-bdaf87ff-6ab3-4ba6-ac23-ad463d5230cd',
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
