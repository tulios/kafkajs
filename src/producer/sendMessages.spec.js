jest.mock('./groupMessagesPerPartition')
const { newLogger } = require('testHelpers')
const { errorCodes, createErrorFromCode } = require('../protocol/error')
const retry = require('../retry')
const createSendMessages = require('./sendMessages')

const createProducerResponse = (topicName, partition, offset = `${partition}`) => ({
  topics: [
    {
      topicName,
      partitions: [
        {
          errorCode: 0,
          offset,
          partition,
          timestamp: '-1',
        },
      ],
    },
  ],
})

describe('Producer > sendMessages', () => {
  const topic = 'topic-name'
  const partitionsPerLeader = {
    1: [0],
    2: [1],
    3: [2],
  }
  let mockProducerId, mockProducerEpoch, mockTransactionalId

  let messages,
    partitioner,
    brokers,
    cluster,
    messagesPerPartition,
    topicPartitionMetadata,
    eosManager,
    retrier

  beforeEach(() => {
    messages = []
    partitioner = jest.fn()
    brokers = {
      1: { nodeId: 1, produce: jest.fn(() => createProducerResponse(topic, 0)) },
      2: { nodeId: 2, produce: jest.fn(() => createProducerResponse(topic, 1)) },
      3: { nodeId: 3, produce: jest.fn(() => createProducerResponse(topic, 2)) },
      4: { nodeId: 4, produce: jest.fn(() => createProducerResponse(topic, 1)) },
    }
    topicPartitionMetadata = [
      {
        isr: [2],
        leader: 1,
        partitionErrorCode: 0,
        partitionId: 0,
        replicas: [2],
      },
    ]

    cluster = {
      addMultipleTargetTopics: jest.fn(),
      refreshMetadata: jest.fn(),
      refreshMetadataIfNecessary: jest.fn(),
      findTopicPartitionMetadata: jest.fn(() => topicPartitionMetadata),
      findLeaderForPartitions: jest.fn(() => partitionsPerLeader),
      findBroker: jest.fn(({ nodeId }) => brokers[nodeId]),
      targetTopics: new Set(),
      isConnected: jest.fn(() => true),
    }
    messagesPerPartition = {
      '0': [{ key: '3' }, { key: '6' }, { key: '9' }],
      '1': [{ key: '1' }, { key: '4' }, { key: '7' }],
      '2': [{ key: '2' }, { key: '5' }, { key: '8' }],
    }

    mockProducerId = -1
    mockProducerEpoch = -1
    mockTransactionalId = undefined

    eosManager = {
      getProducerId: jest.fn(() => mockProducerId),
      getProducerEpoch: jest.fn(() => mockProducerEpoch),
      getSequence: jest.fn(() => 0),
      getTransactionalId: jest.fn(() => mockTransactionalId),
      updateSequence: jest.fn(),
      isTransactional: jest.fn().mockReturnValue(false),
      addPartitionsToTransaction: jest.fn(),
      acquireBrokerLock: jest.fn(),
      releaseBrokerLock: jest.fn(),
    }

    retrier = retry({ retries: 5 })

    require('./groupMessagesPerPartition').mockImplementation(() => messagesPerPartition)
  })

  test('retries every broker after a failed produce', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    brokers[1].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementation(() => createProducerResponse(topic, 0))

    brokers[3].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementation(() => createProducerResponse(topic, 2))

    const response = await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
    expect(eosManager.addPartitionsToTransaction).not.toHaveBeenCalled()

    expect(brokers[1].produce).toHaveBeenCalledTimes(3)
    expect(brokers[2].produce).toHaveBeenCalledTimes(3)
    expect(brokers[3].produce).toHaveBeenCalledTimes(3)
    expect(response).toEqual([
      { errorCode: 0, offset: '0', partition: 0, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '1', partition: 1, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '2', partition: 2, timestamp: '-1', topicName: 'topic-name' },
    ])
  })

  /**
   * responsePerBroker lives outside the retrier. After a sibling NOT_LEADER,
   * metadata can move partition 0 to a new leader and we will Produce there,
   * but the original broker object is still in the map with its first ack.
   * send() concatenates both. The caller sees the stale offset as success even
   * if that append is later truncated.
   */
  test('does not keep a stale produce ack when a sibling NOT_LEADER moves that partition to a new leader', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    const notLeader = createErrorFromCode(
      errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
    )

    brokers[1].produce.mockImplementation(() => createProducerResponse(topic, 0, 'stale-ack'))
    brokers[2].produce
      .mockImplementationOnce(() => {
        throw notLeader
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 1))
    brokers[5] = {
      nodeId: 5,
      produce: jest.fn(() => createProducerResponse(topic, 0, 'new-leader-ack')),
    }

    cluster.findLeaderForPartitions
      .mockImplementationOnce(() => partitionsPerLeader)
      .mockImplementationOnce(() => ({
        5: [0],
        4: [1],
        3: [2],
      }))

    const response = await sendMessages({ topicMessages: [{ topic, messages }] })
    const partition0 = response.filter(({ partition }) => partition === 0)

    expect(brokers[5].produce).toHaveBeenCalled()
    expect(partition0).toHaveLength(1)
    expect(partition0[0]).toMatchObject({
      partition: 0,
      errorCode: 0,
      offset: 'new-leader-ack',
    })
    expect(response).not.toContainEqual(expect.objectContaining({ offset: 'stale-ack' }))
  })

  /**
   * Truncation window: the sibling NOT_LEADER refreshes metadata, but the
   * controller has not yet moved partition 0 off the broker that already
   * acked. That broker is skipped. send() resolves with the first ack.
   * If the broker is then fenced and the append truncated, the caller has
   * already treated the produce as successful.
   */
  test('retries a previously successful broker after a sibling NOT_LEADER when metadata still lists it as leader', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    const notLeader = createErrorFromCode(
      errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
    )

    brokers[1].produce.mockImplementation(() => createProducerResponse(topic, 0, 'zombie-ack'))
    brokers[2].produce
      .mockImplementationOnce(() => {
        throw notLeader
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 1))

    await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(brokers[1].produce).toHaveBeenCalledTimes(2)
  })

  const PRODUCE_ERRORS = [
    'UNKNOWN_TOPIC_OR_PARTITION',
    'LEADER_NOT_AVAILABLE',
    'NOT_LEADER_FOR_PARTITION',
  ]

  for (const errorType of PRODUCE_ERRORS) {
    test(`refresh stale metadata on ${errorType}`, async () => {
      const sendMessages = createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
      brokers[1].produce
        .mockImplementationOnce(() => {
          throw createErrorFromCode(errorCodes.find(({ type }) => type === errorType).code)
        })
        .mockImplementationOnce(() => createProducerResponse(topic, 0))

      await sendMessages({ topicMessages: [{ topic, messages }] })
      expect(brokers[1].produce).toHaveBeenCalledTimes(2)
      expect(cluster.refreshMetadata).toHaveBeenCalled()
    })
  }

  test('does not re-produce messages to brokers that are no longer leaders after metadata refresh', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    brokers[2].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(
          errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
        )
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 0))
    cluster.findLeaderForPartitions
      .mockImplementationOnce(() => partitionsPerLeader)
      .mockImplementationOnce(() => ({
        1: [0],
        4: [1], // Broker 4 replaces broker 2 as leader for partition 1
        3: [2],
      }))

    const response = await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(response).toEqual([
      { errorCode: 0, offset: '0', partition: 0, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '2', partition: 2, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '1', partition: 1, timestamp: '-1', topicName: 'topic-name' },
    ])
  })

  test('refreshes metadata if partition metadata is empty', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('retrieves sequence information from the transaction manager and updates', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    eosManager.getSequence.mockReturnValue(5)

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )
    expect(brokers[2].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )
    expect(brokers[3].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )

    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      0,
      messagesPerPartition[0].length
    )
    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      1,
      messagesPerPartition[1].length
    )
    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      2,
      messagesPerPartition[2].length
    )
  })

  test('adds partitions to the transaction if transactional', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(true)

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    const numTargetBrokers = 3
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledTimes(numTargetBrokers)
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 0 })],
      },
    ])
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 1 })],
      },
    ])
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 2 })],
      },
    ])
  })

  test('if transactional produces with the transactional id and producer id & epoch', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(true)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
  })

  test('if idempotent produces with the producer id & epoch without the transactional id', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(false)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
  })
})
