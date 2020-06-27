const createAdmin = require('../index')

const {
  sslConnectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
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

  test('support SASL PLAIN connections', async () => {
    const cluster = createCluster(saslConnectionOpts(), saslBrokers())
    admin = createAdmin({ cluster, logger: newLogger() })
    await admin.connect()
  })

  test('support SASL SCRAM 256 connections', async () => {
    const cluster = createCluster(saslSCRAM256ConnectionOpts(), saslBrokers())
    admin = createAdmin({ cluster, logger: newLogger() })
    await admin.connect()
  })

  test('support SASL SCRAM 512 connections', async () => {
    const cluster = createCluster(saslSCRAM512ConnectionOpts(), saslBrokers())
    admin = createAdmin({ cluster, logger: newLogger() })
    await admin.connect()
  })
})
