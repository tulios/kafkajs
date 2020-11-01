const RequestV6Protocol = require('./request')

describe('Protocol > Requests > Produce > v6', () => {
  let args

  beforeEach(() => {
    args = {
      acks: 1,
      timeout: 30000,
      transactionalId: null,
      producerId: -1,
      producerEpoch: 0,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-390850453b1c004039ea-1417-1c32a507-edbb-481d-9d9c-e287743f4b74',
          partitions: [
            {
              partition: 0,
              firstSequence: 0,
              messages: [
                {
                  key: 'key-0',
                  value: 'value-0',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a0': 'header-value-a0',
                    'header-b0': 'header-value-b0',
                    'header-c0': 'header-value-c0',
                  },
                },
                {
                  key: 'key-1',
                  value: 'value-1',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a1': 'header-value-a1',
                    'header-b1': 'header-value-b1',
                    'header-c1': 'header-value-c1',
                  },
                },
                {
                  key: 'key-2',
                  value: 'value-2',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a2': 'header-value-a2',
                    'header-b2': 'header-value-b2',
                    'header-c2': 'header-value-c2',
                  },
                },
                {
                  key: 'key-3',
                  value: 'value-3',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a3': 'header-value-a3',
                    'header-b3': 'header-value-b3',
                    'header-c3': 'header-value-c3',
                  },
                },
                {
                  key: 'key-4',
                  value: 'value-4',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a4': 'header-value-a4',
                    'header-b4': 'header-value-b4',
                    'header-c4': 'header-value-c4',
                  },
                },
                {
                  key: 'key-5',
                  value: 'value-5',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a5': 'header-value-a5',
                    'header-b5': 'header-value-b5',
                    'header-c5': 'header-value-c5',
                  },
                },
                {
                  key: 'key-6',
                  value: 'value-6',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a6': 'header-value-a6',
                    'header-b6': 'header-value-b6',
                    'header-c6': 'header-value-c6',
                  },
                },
                {
                  key: 'key-7',
                  value: 'value-7',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a7': 'header-value-a7',
                    'header-b7': 'header-value-b7',
                    'header-c7': 'header-value-c7',
                  },
                },
                {
                  key: 'key-8',
                  value: 'value-8',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a8': 'header-value-a8',
                    'header-b8': 'header-value-b8',
                    'header-c8': 'header-value-c8',
                  },
                },
                {
                  key: 'key-9',
                  value: 'value-9',
                  timestamp: 1509928155660,
                  headers: {
                    'header-a9': 'header-value-a9',
                    'header-b9': 'header-value-b9',
                    'header-c9': 'header-value-c9',
                  },
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
      const request = RequestV6Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV6Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v6_request.json')))
  })
})
