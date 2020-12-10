const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')

const { KafkaJSNonRetriableError } = require('./errors')

const InstrumentationEventEmitter = require('./instrumentation/emitter')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const ConnectionPool = require('./cluster/connectionPool')
const BrokerPool = require('./cluster/brokerPool')
const createConnectionBuilder = require('./cluster/connectionBuilder')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')
const defaultSocketFactory = require('./network/socketFactory')

const PRIVATE = {
  ACQUIRE_BROKERPOOL: Symbol('private:Kafka:acquireBrokerPool'),
  CREATE_BROKERPOOL: Symbol('private:Kafka:createBrokerPool'),
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  CLUSTER_RETRY: Symbol('private:Kafka:clusterRetry'),
  LOGGER: Symbol('private:Kafka:logger'),
  OFFSETS: Symbol('private:Kafka:offsets'),
}

const DEFAULT_METADATA_MAX_AGE = 300000

module.exports = class Client {
  /**
   * @param {Object} options
   * @param {Array<string>} options.brokers example: ['127.0.0.1:9092', '127.0.0.1:9094']
   * @param {Object} options.ssl
   * @param {Object} options.sasl
   * @param {string} options.clientId
   * @param {number} options.connectionTimeout - in milliseconds
   * @param {number} options.authenticationTimeout - in milliseconds
   * @param {number} options.reauthenticationThreshold - in milliseconds
   * @param {number} [options.requestTimeout=30000] - in milliseconds
   * @param {boolean} [options.enforceRequestTimeout]
   * @param {import("../types").RetryOptions} [options.retry]
   * @param {import("../types").ISocketFactory} [options.socketFactory]
   */
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout = 30000,
    enforceRequestTimeout = false,
    retry,
    socketFactory = defaultSocketFactory(),
    logLevel = INFO,
    logCreator = LoggerConsole,
  }) {
    this[PRIVATE.OFFSETS] = new Map()
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CLUSTER_RETRY] = retry
    this[PRIVATE.CREATE_BROKERPOOL] = (
      {
        metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
        allowAutoTopicCreation = true,
        maxInFlightRequests = null,
        instrumentationEmitter = null,
      },
      BrokerPoolType = BrokerPool
    ) => {
      const connectionBuilder = createConnectionBuilder({
        logger: this[PRIVATE.LOGGER],
        retry: this[PRIVATE.CLUSTER_RETRY],
        instrumentationEmitter,
        socketFactory,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        requestTimeout,
        enforceRequestTimeout,
        maxInFlightRequests,
      })
      return new BrokerPoolType({
        logger: this[PRIVATE.LOGGER],
        retry: this[PRIVATE.CLUSTER_RETRY],
        connectionBuilder,
        allowAutoTopicCreation,
        authenticationTimeout,
        reauthenticationThreshold,
        metadataMaxAge,
        instrumentationEmitter,
      })
    }
    this[PRIVATE.ACQUIRE_BROKERPOOL] = ({
      connectionPool,
      instrumentationEmitter,
      ...connectionPoolOptions
    }) => {
      if (
        connectionPool &&
        Object.values(connectionPoolOptions).some(value => typeof value !== 'undefined')
      ) {
        // XXX: We could compare against the actual options of the provided pool ...
        throw new KafkaJSNonRetriableError(
          'Cannot provide both connectionPool and connection pool creation options'
        )
      }

      /** @type {BrokerPool} */
      let brokerPool
      if (connectionPool) {
        brokerPool = connectionPool
        if (instrumentationEmitter) {
          brokerPool.forwardInstrumentationEvents(instrumentationEmitter)
        }
      } else {
        brokerPool = this[PRIVATE.CREATE_BROKERPOOL]({
          ...connectionPoolOptions,
          instrumentationEmitter,
        })
      }
      return brokerPool
    }
    this[PRIVATE.CREATE_CLUSTER] = ({
      isolationLevel,
      connectionPool,
      instrumentationEmitter,
      ...connectionPoolOptions
    }) => {
      const brokerPool = this[PRIVATE.ACQUIRE_BROKERPOOL]({
        connectionPool,
        instrumentationEmitter,
        ...connectionPoolOptions,
      })
      return new Cluster({
        logger: this[PRIVATE.LOGGER],
        retry: this[PRIVATE.CLUSTER_RETRY],
        offsets: this[PRIVATE.OFFSETS],
        isolationLevel,
        targetTopicsLockTimeout: requestTimeout,
        brokerPool,
      })
    }
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
    connectionPool,
  } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      instrumentationEmitter,
      connectionPool,
    })

    return createProducer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
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
    rebalanceTimeout,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    retry = { retries: 5 },
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
    rackId = '',
    connectionPool,
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
      connectionPool,
    })

    return createConsumer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      rebalanceTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      isolationLevel,
      instrumentationEmitter,
      rackId,
      metadataMaxAge,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      allowAutoTopicCreation: false,
      instrumentationEmitter,
    })

    return createAdmin({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      instrumentationEmitter,
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this[PRIVATE.LOGGER]
  }

  connectionPool({
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    allowAutoTopicCreation = true,
    maxInFlightRequests = null,
  }) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    return this[PRIVATE.CREATE_BROKERPOOL](
      {
        metadataMaxAge,
        allowAutoTopicCreation,
        maxInFlightRequests,
        instrumentationEmitter,
      },
      ConnectionPool
    )
  }
}
