jest.mock('./producer')
jest.mock('./cluster')

const Client = require('./index')
const createProducer = require('./producer')

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
    }
    client.producer(options)

    expect(createProducer).toHaveBeenCalledWith(options)
  })
})
