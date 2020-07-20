const createAdmin = require('../index')

const {
  sslConnectionOpts,
  saslEntries,
  createCluster,
  sslBrokers,
  saslBrokers,
  newLogger,
} = require('testHelpers')

describe('Admin', () => {
  let admin

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  test('support SSL connections', async () => {
    const cluster = createCluster(sslConnectionOpts(), sslBrokers())
    admin = createAdmin({ cluster, logger: newLogger() })

    await admin.connect()
  })

  for (const e of saslEntries) {
    test(`support SASL ${e.name} connections`, async () => {
      const cluster = createCluster(e.opts(), saslBrokers())
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()
    })
  }
})
