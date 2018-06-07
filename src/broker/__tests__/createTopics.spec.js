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
          errorCode: 0,
          topic: topicName1,
        },
        {
          errorCode: 0,
          topic: topicName2,
        },
      ].sort(topicNameComparator),
    })
  })
})
