const { decode, parse } = require('./response')
const { KafkaJSNotImplemented } = require('../../../../errors')

describe('Protocol > Requests > Fetch > v4', () => {
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
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-ab4d54774dcadc395a7f',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '3',
              abortedTransactions: [],
              lastStableOffset: '3',
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

  test('response with GZIP', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_gzip.json')))

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-43c95a3dc68dbf78a359',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '6',
              lastStableOffset: '6',
              abortedTransactions: [],
              messages: [
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '0',
                  headers: {},
                  key: Buffer.from('key-0'),
                  value: Buffer.from('some-value-0'),
                  isControlRecord: false,
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '1',
                  headers: {},
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                  isControlRecord: false,
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '2',
                  headers: {},
                  key: Buffer.from('key-2'),
                  value: Buffer.from('some-value-2'),
                  isControlRecord: false,
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '3',
                  headers: {},
                  key: Buffer.from('key-1'),
                  value: Buffer.from('some-value-1'),
                  isControlRecord: false,
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '4',
                  headers: {},
                  key: Buffer.from('key-2'),
                  value: Buffer.from('some-value-2'),
                  isControlRecord: false,
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1509827900073',
                  offset: '5',
                  headers: {},
                  key: Buffer.from('key-3'),
                  value: Buffer.from('some-value-3'),
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

  test('response with 0.10 format', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_010_format.json')))
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic2-08a2f12dddd8c924460f-78767-02073a3e-0622-4d0d-9ee9-b5d6a5a326f1',
          partitions: [
            {
              partition: 1,
              errorCode: 0,
              highWatermark: '1',
              lastStableOffset: '1',
              abortedTransactions: [],
              messages: [
                {
                  offset: '0',
                  size: 158,
                  crc: 2036710961,
                  magicByte: 1,
                  attributes: 0,
                  timestamp: '1538502423117',
                  key: Buffer.from(
                    'key-9bf6284dc11345082649-78767-f79b4780-f2aa-4bbb-979f-9a4815652b5c'
                  ),
                  value: Buffer.from(
                    'value-9bf6284dc11345082649-78767-f79b4780-f2aa-4bbb-979f-9a4815652b5c'
                  ),
                },
              ],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('response with several RecordBatch (from Scala producer)', async () => {
    const data = await decode(
      Buffer.from(require('../fixtures/v4_from_scala_producer_response.json'))
    )
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-bec28e95-0c2f-49d3-a230-2418dceac885',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '6',
              lastStableOffset: '6',
              abortedTransactions: [],
              messages: [
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644731680',
                  offset: '0',
                  key: Buffer.from('KEY-1'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-1'),
                  isControlRecord: false,
                  headers: {},
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644732194',
                  offset: '1',
                  key: Buffer.from('KEY-2'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-2'),
                  isControlRecord: false,
                  headers: {},
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644732699',
                  offset: '2',
                  key: Buffer.from('KEY-3'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-3'),
                  isControlRecord: false,
                  headers: {},
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644733203',
                  offset: '3',
                  key: Buffer.from('KEY-4'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-4'),
                  isControlRecord: false,
                  headers: {},
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644733708',
                  offset: '4',
                  key: Buffer.from('KEY-5'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-5'),
                  isControlRecord: false,
                  headers: {},
                },
                {
                  magicByte: 2,
                  attributes: 0,
                  batchContext,
                  timestamp: '1539644734213',
                  offset: '5',
                  key: Buffer.from('KEY-6'),
                  value: Buffer.from('VALUE-Lorem ipsum dolor sit amet-6'),
                  isControlRecord: false,
                  headers: {},
                },
              ],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  describe('response with mixed formats (0.10 MessageSet + 0.11 RecordBatch)', () => {
    test('decode only the 0.10 messages, 0.11 should be decoded on the next request', async () => {
      const data = await decode(Buffer.from(require('../fixtures/v4_response_mixed_formats.json')))
      const messagesMagicBytes = data.responses[0].partitions[0].messages.map(m => m.magicByte)

      // the fixture is too big, jest deepCheck matcher takes too long to compare the object,
      // and the purpose of the test is to check if the decoder can skip the 0.11 on this request
      expect(new Set(messagesMagicBytes)).toEqual(new Set([1]))
    })
  })

  describe('response with an unconfigured compression codec (snappy)', () => {
    test('throws KafkaJSNotImplemented error', async () => {
      await expect(
        decode(Buffer.from(require('../fixtures/v4_response_snappy.json')))
      ).rejects.toThrow(KafkaJSNotImplemented)
    })
  })
})
