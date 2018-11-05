const Lock = require('../utils/lock')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup } = require('../protocol/requests')
const { KafkaJSNonRetriableError } = require('../errors')
const apiKeys = require('../protocol/requests/apiKeys')
const SASLAuthenticator = require('./saslAuthenticator')

/**
 * Each node in a Kafka cluster is called broker. This class contains
 * the high-level operations a node can perform.
 *
 * @param {Connection} connection
 * @param {Object} logger
 * @param {Object} [versions=null] The object with all available versions and APIs
 *                                 supported by this cluster. The output of broker#apiVersions
 * @param {number} [authenticationTimeout=1000]
 * @param {boolean} [allowAutoTopicCreation=true] If this and the broker config 'auto.create.topics.enable'
 *                                                are true, topics that don't exist will be created when
 *                                                fetching metadata.
 */
module.exports = class Broker {
  constructor({
    connection,
    logger,
    allowExperimentalV011,
    nodeId = null,
    versions = null,
    authenticationTimeout = 1000,
    allowAutoTopicCreation = true,
  }) {
    this.connection = connection
    this.nodeId = nodeId
    this.rootLogger = logger
    this.versions = versions
    this.allowExperimentalV011 = allowExperimentalV011
    this.authenticationTimeout = authenticationTimeout
    this.allowAutoTopicCreation = allowAutoTopicCreation
    this.authenticated = false

    const lockTimeout = this.connection.connectionTimeout + this.authenticationTimeout
    const brokerAddress = `${this.connection.host}:${this.connection.port}`

    this.lock = new Lock({
      timeout: lockTimeout,
      description: `connect to broker ${brokerAddress}`,
    })

    this.lookupRequest = () => {
      throw new Error('Broker not connected')
    }
  }

  /**
   * @public
   * @returns {boolean}
   */
  isConnected() {
    const { connected, sasl } = this.connection
    return sasl ? connected && this.authenticated : connected
  }

  /**
   * @public
   * @returns {Promise}
   */
  async connect() {
    try {
      await this.lock.acquire()

      if (this.isConnected()) {
        return
      }

      this.authenticated = false
      await this.connection.connect()

      if (!this.versions) {
        this.versions = await this.apiVersions()
      }

      this.lookupRequest = lookup(this.versions, this.allowExperimentalV011)

      if (!this.authenticated && this.connection.sasl) {
        await new SASLAuthenticator(this.connection, this.rootLogger, this.versions).authenticate()
        this.authenticated = true
      }
    } finally {
      await this.lock.release()
    }
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.authenticated = false
    await this.connection.disconnect()
  }

  /**
   * @public
   * @returns {Promise}
   */
  async apiVersions() {
    let response
    const availableVersions = requests.ApiVersions.versions
      .map(Number)
      .sort()
      .reverse()

    // Find the best version implemented by the server
    for (let candidateVersion of availableVersions) {
      try {
        const apiVersions = requests.ApiVersions.protocol({ version: candidateVersion })
        response = await this.connection.send(apiVersions())
        break
      } catch (e) {
        if (e.type !== 'UNSUPPORTED_VERSION') {
          throw e
        }
      }
    }

    if (!response) {
      throw new KafkaJSNonRetriableError('API Versions not supported')
    }

    return response.apiVersions.reduce(
      (obj, version) =>
        Object.assign(obj, {
          [version.apiKey]: {
            minVersion: version.minVersion,
            maxVersion: version.maxVersion,
          },
        }),
      {}
    )
  }

  /**
   * @public
   * @param {Array} [topics=[]] An array of topics to fetch metadata for.
   *                            If no topics are specified fetch metadata for all topics
   * @returns {Promise}
   */
  async metadata(topics = []) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(
      metadata({ topics, allowAutoTopicCreation: this.allowAutoTopicCreation })
    )
  }

  /**
   * @public
   * @param {Array} topicData An array of messages per topic and per partition, example:
   *                          [
   *                            {
   *                              topic: 'test-topic-1',
   *                              partitions: [
   *                                {
   *                                  partition: 0,
   *                                  messages: [
   *                                    { key: '1', value: 'A' },
   *                                    { key: '2', value: 'B' },
   *                                  ]
   *                                },
   *                                {
   *                                  partition: 1,
   *                                  messages: [
   *                                    { key: '3', value: 'C' },
   *                                  ]
   *                                }
   *                              ]
   *                            },
   *                            {
   *                              topic: 'test-topic-2',
   *                              partitions: [
   *                                {
   *                                  partition: 4,
   *                                  messages: [
   *                                    { key: '32', value: 'E' },
   *                                  ]
   *                                },
   *                              ]
   *                            },
   *                          ]
   * @param {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @param {number} [timeout=30000] The time to await a response in ms
   * @param {Compression.Types} [compression=Compression.Types.None] Compression codec
   * @returns {Promise}
   */
  async produce({ topicData, acks = -1, timeout = 30000, compression = Compression.None }) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this.connection.send(produce({ acks, timeout, compression, topicData }))
  }

  /**
   * @public
   * @param {number} replicaId=-1 Broker id of the follower. For normal consumers, use -1
   * @param {number} maxWaitTime=5000 Maximum time in ms to wait for the response
   * @param {number} minBytes=1 Minimum bytes to accumulate in the response
   * @param {number} maxBytes=10485760 Maximum bytes to accumulate in the response. Note that this is
   *                                   not an absolute maximum, if the first message in the first non-empty
   *                                   partition of the fetch is larger than this value, the message will still
   *                                   be returned to ensure that progress can be made. Default 10MB.
   * @param {Array} topics Topics to fetch
   *                        [
   *                          {
   *                            topic: 'topic-name',
   *                            partitions: [
   *                              {
   *                                partition: 0,
   *                                fetchOffset: '4124',
   *                                maxBytes: 2048
   *                              }
   *                            ]
   *                          }
   *                        ]
   * @returns {Promise}
   */
  async fetch({ replicaId, maxWaitTime = 5000, minBytes = 1, maxBytes = 10485760, topics }) {
    // TODO: validate topics not null/empty
    const fetch = this.lookupRequest(apiKeys.Fetch, requests.Fetch)
    return await this.connection.send(fetch({ replicaId, maxWaitTime, minBytes, maxBytes, topics }))
  }

  /**
   * @public
   * @param {string} groupId The group id
   * @param {number} groupGenerationId The generation of the group
   * @param {string} memberId The member id assigned by the group coordinator
   * @returns {Promise}
   */
  async heartbeat({ groupId, groupGenerationId, memberId }) {
    const heartbeat = this.lookupRequest(apiKeys.Heartbeat, requests.Heartbeat)
    return await this.connection.send(heartbeat({ groupId, groupGenerationId, memberId }))
  }

  /**
   * @public
   * @param {string} groupId The unique group id
   * @param {CoordinatorType} coordinatorType The type of coordinator to find
   * @returns {Promise}
   */
  async findGroupCoordinator({ groupId, coordinatorType }) {
    // TODO: validate groupId, mandatory
    const findCoordinator = this.lookupRequest(apiKeys.GroupCoordinator, requests.GroupCoordinator)
    return await this.connection.send(findCoordinator({ groupId, coordinatorType }))
  }

  /**
   * @public
   * @param {string} groupId The unique group id
   * @param {number} sessionTimeout The coordinator considers the consumer dead if it receives
   *                                no heartbeat after this timeout in ms
   * @param {string} [memberId=""] The assigned consumer id or an empty string for a new consumer
   * @param {string} [protocolType="consumer"] Unique name for class of protocols implemented by group
   * @param {Array} groupProtocols List of protocols that the member supports (assignment strategy)
   *                                [{ name: 'AssignerName', metadata: '{"version": 1, "topics": []}' }]
   * @returns {Promise}
   */
  async joinGroup({
    groupId,
    sessionTimeout,
    memberId = '',
    protocolType = 'consumer',
    groupProtocols,
  }) {
    // TODO: validate groupId and sessionTimeout (maybe default for sessionTimeout)
    const joinGroup = this.lookupRequest(apiKeys.JoinGroup, requests.JoinGroup)
    return await this.connection.send(
      joinGroup({
        groupId,
        sessionTimeout,
        memberId,
        protocolType,
        groupProtocols,
      })
    )
  }

  /**
   * @public
   * @param {string} groupId
   * @param {string} memberId
   * @returns {Promise}
   */
  async leaveGroup({ groupId, memberId }) {
    const leaveGroup = this.lookupRequest(apiKeys.LeaveGroup, requests.LeaveGroup)
    return await this.connection.send(leaveGroup({ groupId, memberId }))
  }

  /**
   * @public
   * @param {string} groupId
   * @param {number} generationId
   * @param {string} memberId
   * @param {object} groupAssignment
   * @returns {Promise}
   */
  async syncGroup({ groupId, generationId, memberId, groupAssignment }) {
    const syncGroup = this.lookupRequest(apiKeys.SyncGroup, requests.SyncGroup)
    return await this.connection.send(
      syncGroup({
        groupId,
        generationId,
        memberId,
        groupAssignment,
      })
    )
  }

  /**
   * @public
   * @param {number} replicaId=-1 Broker id of the follower. For normal consumers, use -1
   * @param {object} topics e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0 }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async listOffsets({ replicaId, topics }) {
    const listOffsets = this.lookupRequest(apiKeys.ListOffsets, requests.ListOffsets)
    return await this.connection.send(listOffsets({ replicaId, topics }))
  }

  /**
   * @public
   * @param {string} groupId
   * @param {number} groupGenerationId
   * @param {string} memberId
   * @param {number} [retentionTime=-1] -1 signals to the broker that its default configuration
   *                                    should be used.
   * @param {object} topics Topics to commit offsets, e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0, offset: '11' }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async offsetCommit({ groupId, groupGenerationId, memberId, retentionTime, topics }) {
    const offsetCommit = this.lookupRequest(apiKeys.OffsetCommit, requests.OffsetCommit)
    return await this.connection.send(
      offsetCommit({
        groupId,
        groupGenerationId,
        memberId,
        retentionTime,
        topics,
      })
    )
  }

  /**
   * @public
   * @param {string} groupId
   * @param {object} topics e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0 }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async offsetFetch({ groupId, topics }) {
    const offsetFetch = this.lookupRequest(apiKeys.OffsetFetch, requests.OffsetFetch)
    return await this.connection.send(offsetFetch({ groupId, topics }))
  }

  /**
   * @public
   * @param {Array} groupIds
   * @returns {Promise}
   */
  async describeGroups({ groupIds }) {
    const describeGroups = this.lookupRequest(apiKeys.DescribeGroups, requests.DescribeGroups)
    return await this.connection.send(describeGroups({ groupIds }))
  }

  /**
   * @public
   * @param {Array} topics e.g:
   *                 [
   *                   {
   *                     topic: 'topic-name',
   *                     numPartitions: 1,
   *                     replicationFactor: 1
   *                   }
   *                 ]
   * @param {boolean} [validateOnly=false] If this is true, the request will be validated, but the topic
   *                                       won't be created
   * @param {number} [timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise}
   */
  async createTopics({ topics, validateOnly = false, timeout = 5000 }) {
    const createTopics = this.lookupRequest(apiKeys.CreateTopics, requests.CreateTopics)
    return await this.connection.send(createTopics({ topics, validateOnly, timeout }))
  }

  /**
   * @public
   * @param {Array<string>} topics An array of topics to be deleted
   * @param {number} [timeout=5000] The time in ms to wait for a topic to be completely deleted on the
   *                                controller node. Values <= 0 will trigger topic deletion and return
   *                                immediately
   * @returns {Promise}
   */
  async deleteTopics({ topics, timeout = 5000 }) {
    const deleteTopics = this.lookupRequest(apiKeys.DeleteTopics, requests.DeleteTopics)
    return await this.connection.send(deleteTopics({ topics, timeout }))
  }

  /**
   * @public
   * @param {Array<ResourceQuery>} resources
   *                                 [{
   *                                   type: RESOURCE_TYPES.TOPIC,
   *                                   name: 'topic-name',
   *                                   configNames: ['compression.type', 'retention.ms']
   *                                 }]
   * @returns {Promise}
   */
  async describeConfigs({ resources }) {
    const describeConfigs = this.lookupRequest(apiKeys.DescribeConfigs, requests.DescribeConfigs)
    return await this.connection.send(describeConfigs({ resources }))
  }

  /**
   * @public
   * @param {Array<ResourceConfig>} resources
   *                                 [{
   *                                  type: RESOURCE_TYPES.TOPIC,
   *                                  name: 'topic-name',
   *                                  configEntries: [
   *                                    {
   *                                      name: 'cleanup.policy',
   *                                      value: 'compact'
   *                                    }
   *                                  ]
   *                                 }]
   * @param {boolean} [validateOnly=false]
   * @returns {Promise}
   */
  async alterConfigs({ resources, validateOnly = false }) {
    const alterConfigs = this.lookupRequest(apiKeys.AlterConfigs, requests.AlterConfigs)
    return await this.connection.send(alterConfigs({ resources, validateOnly }))
  }

  /**
   * @public
   * @param {number} transactionTimeout The time in ms to wait for before aborting idle transactions
   * @param {number} [transactionalId] The transactional id or null if the producer is not transactional
   * @returns {Promise}
   */
  async initProducerId({ transactionalId, transactionTimeout }) {
    const initProducerId = this.lookupRequest(apiKeys.InitProducerId, requests.InitProducerId)
    return await this.connection.send(initProducerId({ transactionalId, transactionTimeout }))
  }

  /**
   * @public
   * @param {string} transactionalId The transactional id corresponding to the transaction.
   * @param {number} producerId Current producer id in use by the transactional id.
   * @param {number} producerEpoch Current epoch associated with the producer id.
   * @param {object[]} topics e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [ 0, 1]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics }) {
    const addPartitionsToTxn = this.lookupRequest(
      apiKeys.AddPartitionsToTxn,
      requests.AddPartitionsToTxn
    )
    return await this.connection.send(
      addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics })
    )
  }

  /**
   * @public
   * @param {string} transactionalId The transactional id corresponding to the transaction.
   * @param {number} producerId Current producer id in use by the transactional id.
   * @param {number} producerEpoch Current epoch associated with the producer id.
   * @param {string} groupId The unique group identifier (consumer group)
   * @returns {Promise}
   */
  async addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId }) {
    const addOffsetsToTxn = this.lookupRequest(apiKeys.AddOffsetsToTxn, requests.AddOffsetsToTxn)
    return await this.connection.send(
      addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId })
    )
  }
}
