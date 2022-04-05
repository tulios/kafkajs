const { decode, parse } = require('./response')

describe('Protocol > Requests > Fetch > v11', () => {
  const batchContext = {
    firstOffset: expect.any(String),
    firstSequence: expect.any(Number),
    firstTimestamp: expect.any(String),
    inTransaction: expect.any(Boolean),
    isControlBatch: expect.any(Boolean),
    lastOffsetDelta: expect.any(Number),
    magicByte: expect.any(Number),
    maxTimestamp: expect.any(String),
    partitionLeaderEpoch: expect.any(Number),
    producerEpoch: expect.any(Number),
    producerId: expect.any(String),
    timestampType: expect.any(Number),
  }

  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      sessionId: 0,
      responses: [
        {
          topicName: 'test-topic-2077b9d2b36c4082e594-4020-b5a52b27-56df-4b87-800d-82c1cf26317d',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '3',
              abortedTransactions: [],
              lastStableOffset: '3',
              lastStartOffset: '0',
              preferredReadReplica: 0,
              messages: [
                {
                  offset: '0',
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  headers: { 'header-key-0': Buffer.from('header-value-0') },
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                  isControlRecord: false,
                },
                {
                  offset: '1',
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  headers: { 'header-key-1': Buffer.from('header-value-1') },
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                  isControlRecord: false,
                },
                {
                  offset: '2',
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  headers: { 'header-key-2': Buffer.from('header-value-2') },
                  key: Buffer.from('key-2'),
                  value: Buffer.from('some-value-2'),
                  isControlRecord: false,
                },
              ],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
