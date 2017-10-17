const { createConnection, connectionOpts } = require('../../testHelpers')
const Broker = require('./index')

describe('Broker > ApiVersions', () => {
  let broker

  beforeEach(async () => {
    broker = new Broker(createConnection(connectionOpts()))
    await broker.connect()
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await expect(broker.apiVersions()).resolves.toBeTruthy()
  })
})
