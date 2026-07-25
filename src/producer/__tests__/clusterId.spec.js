const { newLogger, createCluster } = require('testHelpers')
const createProducer = require('../index')

describe('Producer > clusterId', () => {
  let producer

  beforeEach(async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
  })

  test('returns null before connecting', () => {
    expect(producer.clusterId()).toBeNull()
  })

  test('returns a non-empty string after connecting', async () => {
    await producer.connect()
    const id = producer.clusterId()
    expect(id).toBeTruthy()
    expect(typeof id).toBe('string')
  })
})
