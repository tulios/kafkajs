jest.mock('./producer')
jest.mock('./consumer')
jest.mock('./cluster')

const Client = require('./index')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const Cluster = require('./cluster')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')

describe('Client', () => {
  it('gives access to its logger', () => {
    expect(new Client({ brokers: [] }).logger()).toMatchSnapshot()
  })

  it('passes options to the producer', () => {
    const client = new Client({ brokers: [] })
    const options = {
      cluster: expect.any(Object),
      logger: expect.any(Object),
      createPartitioner: () => 0,
      retry: { retries: 10 },
      idempotent: true,
      transactionalId: 'transactional-id',
      transactionTimeout: 1,
      instrumentationEmitter: expect.any(Object),
    }
    client.producer(options)

    expect(createProducer).toHaveBeenCalledWith(options)
  })

  describe('consumer', () => {
    test('creates a consumer with the correct isolation level', () => {
      const client = new Client({ brokers: [] })

      const readCommittedConsumerOptions = {
        readUncommitted: false,
      }
      const readUncommittedConsumerOptions = {
        readUncommitted: true,
      }
      client.consumer(readCommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )

      createConsumer.mockClear()
      Cluster.mockClear()
      client.consumer(readUncommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
    })
  })
})
