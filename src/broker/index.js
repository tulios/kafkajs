const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup } = require('../protocol/requests')
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
 */
module.exports = class Broker {
  constructor({ connection, logger, nodeId = null, versions = null }) {
    this.connection = connection
    this.nodeId = nodeId
    this.rootLogger = logger
    this.versions = versions
    this.authenticated = false
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
    this.authenticated = false
    await this.connection.connect()

    if (!this.versions) {
      this.versions = await this.apiVersions()
    }

    this.lookupRequest = lookup(this.versions)

    if (!this.authenticated && this.connection.sasl) {
      await new SASLAuthenticator(this.connection, this.rootLogger, this.versions).authenticate()
      this.authenticated = true
    }

    return true
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
    const apiVersions = requests.ApiVersions.protocol({ version: 0 })
    const response = await this.connection.send(apiVersions())
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
    return await this.connection.send(metadata(topics))
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
   * @returns {Promise}
   */
  async findGroupCoordinator({ groupId }) {
    // TODO: validate groupId, mandatory
    const findCoordinator = this.lookupRequest(apiKeys.GroupCoordinator, requests.GroupCoordinator)
    return await this.connection.send(findCoordinator({ groupId }))
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
   * @param {number} [timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise}
   */
  async createTopics({ topics, timeout = 5000 }) {
    const createTopics = this.lookupRequest(apiKeys.CreateTopics, requests.CreateTopics)
    return await this.connection.send(createTopics({ topics, timeout }))
  }
}
