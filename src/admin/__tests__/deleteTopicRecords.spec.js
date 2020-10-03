const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')

const {
  createCluster,
  newLogger,
  createTopic,
  secureRandom,
  createModPartitioner,
  waitForNextEvent,
} = require('testHelpers')

const Broker = require('../../broker')
const { KafkaJSProtocolError } = require('../../errors')

const { assign } = Object

const logger = assign(newLogger(), { namespace: () => logger })
jest.spyOn(logger, 'warn')

describe('Admin > deleteTopicRecords', () => {
  let topicName, cluster, admin, producer, recordsToDelete, consumer, groupId

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
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
    jest.resetAllMocks()
  })

  afterAll(jest.restoreAllMocks)

  test('throws an error if the topic name is invalid', async () => {
    await expect(admin.deleteTopicRecords({ topic: null })).rejects.toHaveProperty(
      'message',
      'Invalid topic null'
    )

    await expect(admin.deleteTopicRecords({ topic: ['topic-in-an-array'] })).rejects.toHaveProperty(
      'message',
      'Invalid topic topic-in-an-array'
    )
  })

  test('throws an error if the partitions array is invalid', async () => {
    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: [] })
    ).rejects.toHaveProperty('message', 'Invalid partitions')
  })

  test('removes deleted offsets from the selected partition', async () => {
    recordsToDelete = [{ partition: 0, offset: '7' }]
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
    recordsToDelete = [{ partition: 0, offset: '7' }]
    const messagesPartition0 = []
    const messagesPartition1 = []
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    consumer.run({
      eachMessage: async event => {
        if (event.partition === 0) messagesPartition0.push(event)
        if (event.partition === 1) messagesPartition1.push(event)
      },
    })
    // wait for the 2 partition batches to get processed
    await waitForNextEvent(consumer, consumer.events.END_BATCH_PROCESS)
    await waitForNextEvent(consumer, consumer.events.END_BATCH_PROCESS)

    expect(messagesPartition0.length).toBe(6) // 13 original minus 7 deleted
    expect(messagesPartition1.length).toBe(7) // original number of messages
  })

  test('deletes all records when provided the -1 offset', async () => {
    recordsToDelete = [{ partition: 0, offset: '-1' }]

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
    recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 1, offset: '5' },
    ]
    const brokerSpy = jest.spyOn(Broker.prototype, 'deleteRecords')
    brokerSpy.mockResolvedValueOnce() // succeed once
    brokerSpy.mockRejectedValueOnce(new KafkaJSProtocolError('retriable error')) // fail once

    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    // broker call #1 succeeds, broker call #2 fails, call #3 should be the last one (skips broker #1, and only retries #2)
    expect(brokerSpy).toHaveBeenCalledTimes(3)
    expect(brokerSpy.mock.calls[1]).not.toEqual(brokerSpy.mock.calls[0])
    expect(brokerSpy.mock.calls[2]).toEqual(brokerSpy.mock.calls[1])
    brokerSpy.mockRestore()
  })

  test('in case offset is out of range, throws before processing any partitions', async () => {
    recordsToDelete = [{ partition: 0, offset: '99' }]
    const brokerSpy = jest.spyOn(Broker.prototype, 'deleteRecords')

    await expect(
      admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })
    ).rejects.toThrow(
      new KafkaJSProtocolError(
        'The requested offset is not within the range of offsets maintained by the server'
      )
    )
    expect(brokerSpy).not.toHaveBeenCalled()
    brokerSpy.mockRestore()
  })

  test('in case offset is below low watermark, log a warning', async () => {
    // delete #1 to set the low watermark to 5
    recordsToDelete = [{ partition: 1, offset: '5' }]
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

  test('in case partition does not exist/has no lead broker, skip the partition with a warning', async () => {
    // try to add to the delete request a partition that doesn't exist
    recordsToDelete = [
      { partition: 0, offset: '7' },
      { partition: 2, offset: '5' },
    ]
    await admin.deleteTopicRecords({ topic: topicName, partitions: recordsToDelete })

    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(logger.warn).toHaveBeenCalledWith('Could not find broker/leader for the partition', {
      topic: topicName,
      partition: 2,
    })
    // ignore the incorrect partition, and delete the correct partition's selected messages
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
        partitions: [{ partition: 0, offset: '7' }],
      },
    ])
  })
})
