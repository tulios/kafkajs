const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')

const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')

const PRIVATE = {
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  LOGGER: Symbol('private:Kafka:logger'),
}

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    retry,
    logLevel = INFO,
    logCreator = LoggerConsole,
    allowExperimentalV011 = false,
  }) {
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CREATE_CLUSTER] = (metadataMaxAge = 300000) =>
      new Cluster({
        logger: this[PRIVATE.LOGGER],
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        metadataMaxAge,
        retry,
        allowExperimentalV011,
      })
  }

  /**
   * @public
   */
  producer({ createPartitioner, retry, metadataMaxAge } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER](metadataMaxAge)
    return createProducer({
      retry: { ...cluster.retry, ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
    })
  }

  /**
   * @public
   */
  consumer({
    groupId,
    partitionAssigners,
    metadataMaxAge,
    sessionTimeout,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    retry,
  } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER](metadataMaxAge)
    return createConsumer({
      retry: { ...cluster.retry, retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER]()
    return createAdmin({
      retry: { ...cluster.retry, retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this[PRIVATE.LOGGER]
  }
}
