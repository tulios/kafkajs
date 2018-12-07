const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')

const InstrumentationEventEmitter = require('./instrumentation/emitter')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')

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
    requestTimeout,
    retry,
    logLevel = INFO,
    logCreator = LoggerConsole,
    allowExperimentalV011 = true,
  }) {
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CREATE_CLUSTER] = ({
      metadataMaxAge = 300000,
      allowAutoTopicCreation = true,
      maxInFlightRequests = null,
      instrumentationEmitter = null,
      isolationLevel,
    }) =>
      new Cluster({
        logger: this[PRIVATE.LOGGER],
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        requestTimeout,
        metadataMaxAge,
        instrumentationEmitter,
        retry,
        allowAutoTopicCreation,
        allowExperimentalV011,
        maxInFlightRequests,
        isolationLevel,
      })
  }

  /**
   * @public
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
  } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      instrumentationEmitter,
    })

    return createProducer({
      retry: { ...cluster.retry, ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
      idempotent,
      transactionalId,
      transactionTimeout,
      instrumentationEmitter,
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
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
  } = {}) {
    const isolationLevel = readUncommitted
      ? ISOLATION_LEVEL.READ_UNCOMMITTED
      : ISOLATION_LEVEL.READ_COMMITTED

    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      isolationLevel,
      instrumentationEmitter,
    })

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
      isolationLevel,
      instrumentationEmitter,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER]({ allowAutoTopicCreation: false })
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
