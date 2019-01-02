const createAdmin = require('./index')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { createCluster, newLogger, secureRandom } = require('testHelpers')

describe('Admin', () => {
  it('gives access to its logger', () => {
    expect(
      createAdmin({
        cluster: createCluster(),
        logger: newLogger(),
      }).logger()
    ).toMatchSnapshot()
  })

  it('emits connection events', async () => {
    const admin = createAdmin({
      cluster: createCluster(),
      logger: newLogger(),
    })

    const connectListener = jest.fn().mockName('connect')
    const disconnectListener = jest.fn().mockName('disconnect')
    admin.on(admin.events.CONNECT, connectListener)
    admin.on(admin.events.DISCONNECT, disconnectListener)

    await admin.connect()
    expect(connectListener).toHaveBeenCalled()

    await admin.disconnect()
    expect(disconnectListener).toHaveBeenCalled()
  })

  test('emits the request event', async () => {
    const emitter = new InstrumentationEventEmitter()
    const admin = createAdmin({
      cluster: createCluster({ instrumentationEmitter: emitter }),
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request')
    admin.on(admin.events.REQUEST, requestListener)

    await admin.connect()
    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'admin.network.request',
      payload: {
        apiKey: expect.any(Number),
        apiName: 'ApiVersions',
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        duration: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
        size: expect.any(Number),
      },
    })
  })

  test('emits the request timeout event', async () => {
    const emitter = new InstrumentationEventEmitter()
    const cluster = createCluster({
      requestTimeout: 1,
      instrumentationEmitter: emitter,
    })

    const admin = createAdmin({
      cluster,
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })

    const requestListener = jest.fn().mockName('request_timeout')
    admin.on(admin.events.REQUEST_TIMEOUT, requestListener)

    await admin
      .connect()
      .then(() =>
        admin.createTopics({
          waitForLeaders: false,
          topics: [{ topic: `test-topic-${secureRandom()}` }],
        })
      )
      .catch(e => e)

    expect(requestListener).toHaveBeenCalledWith({
      id: expect.any(Number),
      timestamp: expect.any(Number),
      type: 'admin.network.request_timeout',
      payload: {
        apiKey: expect.any(Number),
        apiName: expect.any(String),
        apiVersion: expect.any(Number),
        broker: expect.any(String),
        clientId: expect.any(String),
        correlationId: expect.any(Number),
        createdAt: expect.any(Number),
        pendingDuration: expect.any(Number),
        sentAt: expect.any(Number),
      },
    })
  })

  test('on throws an error when provided with an invalid event name', () => {
    const admin = createAdmin({
      cluster: createCluster(),
      logger: newLogger(),
    })

    expect(() => admin.on('NON_EXISTENT_EVENT', () => {})).toThrow(
      /Event name should be one of admin.events./
    )
  })
})
