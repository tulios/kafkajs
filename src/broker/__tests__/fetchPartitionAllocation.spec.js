jest.mock('../../utils/shuffle', () => jest.fn())

const Broker = require('../index')
const mockShuffle = require('../../utils/shuffle')

const { createConnectionPool, newLogger } = require('testHelpers')

const minBytes = 1
const maxBytes = 10485760 // 10MB
const maxBytesPerPartition = 1048576 // 1MB
const maxWaitTime = 100

describe('Broker > Fetch', () => {
  let broker, connection, fetchRequestMock

  const createFetchTopic = (name, numPartitions) => {
    const partitions = Array(numPartitions)
      .fill()
      .map((_, i) => ({
        partition: i,
        fetchOffset: 0,
        maxBytes: maxBytesPerPartition,
      }))

    return {
      topic: name,
      partitions,
    }
  }

  beforeEach(async () => {
    connection = createConnectionPool()
    connection.send = jest.fn()
    broker = new Broker({
      connectionPool: connection,
      logger: newLogger(),
    })

    broker.lookupRequest = () => fetchRequestMock
  })

  /**
   * When requesting data for topic-partitions with more data than `maxBytes`,
   * the response will be allocated in the order
   * the topic-partitions appear in the request. To avoid biasing consumption,
   * the order of all topic-partitions is randomized on each request.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes
   */
  test('it should randomize the order of the requested topic-partitions', async () => {
    const topics = [createFetchTopic('topic-a', 3), createFetchTopic('topic-b', 2)]
    const flattened = [
      { topic: topics[0].topic, partition: topics[0].partitions[0] },
      { topic: topics[0].topic, partition: topics[0].partitions[1] },
      { topic: topics[0].topic, partition: topics[0].partitions[2] },
      { topic: topics[1].topic, partition: topics[1].partitions[0] },
      { topic: topics[1].topic, partition: topics[1].partitions[1] },
    ]

    mockShuffle.mockImplementationOnce(() => [
      flattened[0],
      flattened[3],
      // test that consecutive partitions for same topic are merged
      flattened[1],
      flattened[2],
      flattened[4],
    ])
    fetchRequestMock = jest.fn().mockReturnValue({ request: {} })

    await broker.fetch({ maxWaitTime, minBytes, maxBytes, topics })

    expect(mockShuffle).toHaveBeenCalledWith(flattened)

    expect(fetchRequestMock).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: [
          { topic: topics[0].topic, partitions: [topics[0].partitions[0]] },
          { topic: topics[1].topic, partitions: [topics[1].partitions[0]] },
          {
            topic: topics[0].topic,
            partitions: [topics[0].partitions[1], topics[0].partitions[2]],
          },
          { topic: topics[1].topic, partitions: [topics[1].partitions[1]] },
        ],
      })
    )
  })
})
