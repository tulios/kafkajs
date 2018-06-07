const { createConnection, connectionOpts, secureRandom, newLogger } = require('testHelpers')

const Broker = require('../index')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > createTopics', () => {
  let broker

  beforeEach(() => {
    broker = new Broker({
      connection: createConnection(connectionOpts()),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    expect(response).toEqual({
      topicErrors: [
        {
          topic: topicName1,
          errorCode: 0,
          errorMessage: null,
        },
        {
          topic: topicName2,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })

  test('request with validateOnly', async () => {
    await broker.connect()
    const topicName = `test-topic-${secureRandom()}`
    const response = await broker.createTopics({
      topics: [{ topic: topicName }],
      validateOnly: true,
    })

    expect(response).toEqual({
      topicErrors: [
        {
          topic: topicName,
          errorCode: 0,
          errorMessage: null,
        },
      ].sort(topicNameComparator),
    })
  })
})
