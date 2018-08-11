const createAdmin = require('./index')
const { createCluster, newLogger } = require('testHelpers')
const { KafkaJSNonRetriableError } = require('../errors')

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
