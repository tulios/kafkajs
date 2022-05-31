const RequestV3Protocol = require('./request')

describe('Protocol > Requests > Produce > v3', () => {
  let args

  beforeEach(() => {
    args = {
      transactionalId: null,
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-ebba68879c6f5081d8c2',
          partitions: [
            {
              partition: 0,
              firstSequence: 0,
              messages: [
                {
                  key: 'key-9d0f348cb2e730e1edc4',
                  value: 'some-value-a17b4c81f9ecd1e896e3',
                  timestamp: 1509928155660,
                  headers: { a: 'b', c: ['d', 'e'] },
                },
              ],
            },
          ],
        },
      ],
    }
  })

  describe('when acks=0', () => {
    test('expectResponse returns false', () => {
      const request = RequestV3Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      transactionalId: null,
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-ebba68879c6f5081d8c2',
          partitions: [
            {
              partition: 0,
              firstSequence: 10,
              messages: [
                {
                  key: 'key-9d0f348cb2e730e1edc4',
                  value: 'some-value-a17b4c81f9ecd1e896e3',
                  timestamp: 1509928155660,
                  headers: { a: 'b', c: ['d', 'e'] },
                },
                {
                  key: 'key-c7073e965c34b4cc6442',
                  value: 'some-value-65df422070d7ad73914f',
                  timestamp: 1509928155660,
                  headers: { a: 'b' },
                },
                {
                  key: 'key-1693b184a9b52dbe03bc',
                  value: 'some-value-3fcb65ffca087cba20ad',
                  timestamp: 1509928155660,
                  headers: { a: 'b' },
                },
              ],
            },
          ],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })
})
