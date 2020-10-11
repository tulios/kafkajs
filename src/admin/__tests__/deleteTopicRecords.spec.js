const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')

const {
  createCluster,
  newLogger,
  createTopic,
  secureRandom,
  createModPartitioner,
  waitForMessages,
} = require('testHelpers')

const Broker = require('../../broker')
const {
  KafkaJSProtocolError,
  KafkaJSOffsetOutOfRange,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSBrokerNotFound,
  KafkaJSError,
} = require('../../errors')

const { assign } = Object

const STALE_METADATA_ERRORS = [
  { type: 'UNKNOWN_TOPIC_OR_PARTITION' },
  { type: 'LEADER_NOT_AVAILABLE' },
  { type: 'NOT_LEADER_FOR_PARTITION' },
  { name: 'KafkaJSMetadataNotLoaded' },
]

const logger = assign(newLogger(), { namespace: () => logger })
jest.spyOn(logger, 'warn')

describe('Admin > deleteTopicRecords', () => {
  let topicName, cluster, admin, producer, consumer, groupId, brokerSpy, metadataSpy

  // used to test expected values that could be 1 of 2 possibilities
  expect.extend({
    toBeEither(received, first, second) {
      const message = () => `expected ${received} to be either ${first} or ${second}`
      const pass = received === first || received === second
      if (pass) {
        return { message, pass: true }
      } else {
        return { message, pass: false }
      }
    },
  })

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    cluster = createCluster()
    admin = createAdmin({ cluster: cluster, logger })

    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger,
    })

    await Promise.all([admin.connect(), producer.connect()])

    await createTopic({ topic: topicName, partitions: 2 })

    const messages = Array(20)
      .fill()
      .map((e, i) => {
        const value = secureRandom()
        return { key: `key-${i}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })

    groupId = `consumer-group-id-${secureRandom()}`
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger,
    })
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    // validate the modulus partitioner allocates 20 messages 13:7
    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: false,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '13' },
          { partition: 1, offset: '7' },
        ]),
      },
    ])

    brokerSpy = jest.spyOn(Broker.prototype, 'deleteRecords')
    metadataSpy = jest.spyOn(cluster, 'refreshMetadata')
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    jest.resetAllMocks()
    brokerSpy && brokerSpy.mockRestore()
    metadataSpy && metadataSpy.mockRestore()
  })

  afterAll(jest.restoreAllMocks)

  test('throws an error if the topic name is invalid', async () => {
    await expect(admin.deleteTopicRecords({ topic: null })).rejects.toHaveProperty(
      'message',
      'Invalid topic "null"'
    )

    await expect(admin.deleteTopicRecords({ topic: ['topic-in-an-array'] })).rejects.toHaveProperty(
      'message',
      'Invalid topic "topic-in-an-array"'
    )
  })

  test('throws an error if the partitions array is invalid', async () => {
    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: [] })
    ).rejects.toHaveProperty('message', 'Invalid partitions')
  })

  test('removes deleted offsets from the selected partition', async () => {
    const recordsToDelete = [{ partition: 0, offset: '7' }]
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '7' },
          { partition: 1, offset: '0' },
        ]),
      },
    ])
  })

  test('non-deleted messages are successfully consumed', async () => {
    const recordsToDelete = [{ partition: 0, offset: '7' }]
    const messagesConsumed = []
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    consumer.run({
      eachMessage: async event => {
        messagesConsumed.push(event)
      },
    })
    await waitForMessages(messagesConsumed, { number: 13 })

    expect(messagesConsumed.filter(({ partition }) => partition === 0)).toHaveLength(6) // 13 original minus 7 deleted
    expect(messagesConsumed.find(({ partition }) => partition === 0)).toEqual(
      expect.objectContaining({
        message: expect.objectContaining({ offset: '7' }), // first message is offset 7
      })
    )
    expect(
      messagesConsumed
        .slice()
        .reverse()
        .find(({ partition }) => partition === 0)
    ).toEqual(
      expect.objectContaining({
        message: expect.objectContaining({ offset: '12' }), // last message is offset 12
      })
    )

    expect(messagesConsumed.filter(({ partition }) => partition === 1)).toHaveLength(7) // original number of messages
  })

  test('deletes all records when provided the -1 offset', async () => {
    const recordsToDelete = [{ partition: 0, offset: '-1' }]

    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: '13' }],
      },
    ])
  })

  test('in case of retriable error, tries again from the last successful partition (does not re-process successful partition twice)', async () => {
    // tries to delete from partition 0 AND partition 1 -> tries to call broker.deleteRecords twice
    const recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 1, offset: '5' },
    ]
    brokerSpy.mockResolvedValueOnce() // succeed once
    brokerSpy.mockRejectedValueOnce(new KafkaJSProtocolError('retriable error')) // fail once

    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    // broker call #1 succeeds, broker call #2 fails, call #3 should be the last one (skips broker #1, and only retries #2)
    expect(brokerSpy).toHaveBeenCalledTimes(3)
    expect(brokerSpy.mock.calls[1]).not.toEqual(brokerSpy.mock.calls[0])
    expect(brokerSpy.mock.calls[2]).toEqual(brokerSpy.mock.calls[1])
  })

  for (const error of STALE_METADATA_ERRORS) {
    test(`${error.type || error.name} refresh stale metadata and tries again`, async () => {
      class FakeError extends Error {
        constructor() {
          super('Fake Error')
          this.name = error.name
          this.type = error.type
          this.retriable = true
        }
      }
      brokerSpy.mockRejectedValueOnce(new FakeError())

      const recordsToDelete = [{ partition: 1, offset: '5' }]
      await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

      expect(brokerSpy).toHaveBeenCalledTimes(2)
      expect(metadataSpy).toHaveBeenCalled()
    })
  }

  test('in case at least one partition does not exist/has no leader, throws before processing any partitions', async () => {
    // try to add to the delete request a partition that doesn't exist
    const recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 2, offset: '5' },
    ]
    const expectedError = new KafkaJSDeleteTopicRecordsError({
      topic: topicName,
      brokers: [
        {
          partitions: [
            {
              partition: 2,
              offset: '5',
            },
          ],
          error: new KafkaJSBrokerNotFound('Could not find leader for the partition'),
        },
      ],
    })

    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    ).rejects.toThrow(expectedError)
    expect(brokerSpy).not.toHaveBeenCalled()
  })

  test('in case offset is below low watermark, log a warning', async () => {
    // delete #1 to set the low watermark to 5
    let recordsToDelete = [{ partition: 1, offset: '5' }]
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    // delete #2
    recordsToDelete = [
      { partition: 0, offset: '7' }, // work as normal
      { partition: 1, offset: '3' }, // logs a warning + no effect on the partition
    ]
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(
      logger.warn
    ).toHaveBeenCalledWith(
      'The requested offset is before the earliest offset maintained on the partition - no records will be deleted from this partition',
      { topic: topicName, partition: 1, offset: '3' }
    )
    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '7' },
          { partition: 1, offset: '5' },
        ]),
      },
    ])
  })

  test('if 1 of the broker requests throws a non-retriable, an error is thrown for the unsuccessful request', async () => {
    const recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 1, offset: '99' },
    ]
    const expectedError = new KafkaJSDeleteTopicRecordsError({
      topic: topicName,
      brokers: [
        {
          partitions: { partition: 1, offset: '99' },
          error: new KafkaJSOffsetOutOfRange(
            'The requested offset is not within the range of offsets maintained by the server',
            { topic: topicName, partition: 0 }
          ),
        },
      ],
    })
    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    ).rejects.toThrow(expectedError)
    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '7' },
          { partition: 1, offset: '0' },
        ]),
      },
    ])
  })

  test('if at least 1 broker error is non-retriable, will not attempt to retry', async () => {
    const recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 1, offset: '5' },
    ]
    brokerSpy.mockRejectedValueOnce(new KafkaJSError('Fake Error', { retriable: true }))
    brokerSpy.mockRejectedValueOnce(new KafkaJSError('Fake Error', { retriable: false }))

    const expectedError = new KafkaJSDeleteTopicRecordsError({
      topic: topicName,
      brokers: [
        {
          partitions: expect.toBeEither(
            [{ partition: 0, offset: '7' }],
            [{ partition: 1, offset: '5' }]
          ),
          error: new KafkaJSOffsetOutOfRange(
            'The requested offset is not within the range of offsets maintained by the server',
            { topic: topicName, partition: 0 }
          ),
        },
        {
          partitions: expect.toBeEither(
            [{ partition: 0, offset: '7' }],
            [{ partition: 1, offset: '5' }]
          ),
          error: new KafkaJSOffsetOutOfRange(
            'The requested offset is not within the range of offsets maintained by the server',
            { topic: topicName, partition: 0 }
          ),
        },
      ],
    })
    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    ).rejects.toThrow(expectedError)
    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '0' },
          { partition: 1, offset: '0' },
        ]),
      },
    ])
  })

  test('if all broker errors are retriable, will retry the request', async () => {
    const recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 1, offset: '5' },
    ]
    brokerSpy.mockRejectedValueOnce(new KafkaJSError('Fake Error', { retriable: true }))
    brokerSpy.mockRejectedValueOnce(new KafkaJSError('Fake Error', { retriable: true }))

    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    expect(
      await cluster.fetchTopicsOffset([
        {
          topic: topicName,
          partitions: [{ partition: 0 }, { partition: 1 }],
          fromBeginning: true,
        },
      ])
    ).toEqual([
      {
        topic: topicName,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '7' },
          { partition: 1, offset: '5' },
        ]),
      },
    ])
  })
})
