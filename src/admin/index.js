const createRetry = require('../retry')
const flatten = require('../utils/flatten')
const waitFor = require('../utils/waitFor')
const createConsumer = require('../consumer')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const { LEVELS } = require('../loggers')
const { KafkaJSNonRetriableError, KafkaJSDeleteGroupsError } = require('../errors')
const RESOURCE_TYPES = require('../protocol/resourceTypes')

const { CONNECT, DISCONNECT } = events

const NO_CONTROLLER_ID = -1

const { values, keys } = Object
const eventNames = values(events)
const eventKeys = keys(events)
  .map(key => `admin.events.${key}`)
  .join(', ')

const retryOnLeaderNotAvailable = (fn, opts = {}) => {
  const callback = async () => {
    try {
      return await fn()
    } catch (e) {
      if (e.type !== 'LEADER_NOT_AVAILABLE') {
        throw e
      }
      return false
    }
  }

  return waitFor(callback, opts)
}

const isConsumerGroupRunning = description => ['Empty', 'Dead'].includes(description.state)
const findTopicPartitions = async (cluster, topic) => {
  await cluster.addTargetTopic(topic)
  await cluster.refreshMetadataIfNecessary()

  return cluster
    .findTopicPartitionMetadata(topic)
    .map(({ partitionId }) => partitionId)
    .sort()
}

/**
 *
 * @param {Object} params
 * @param {import("../../types").Logger} params.logger
 * @param {import('../instrumentation/emitter')} [params.instrumentationEmitter]
 * @param {import('../../types').RetryOptions} params.retry
 * @param {import("../../types").Cluster} params.cluster
 *
 * @returns {import("../../types").Admin}
 */
module.exports = ({
  logger: rootLogger,
  instrumentationEmitter: rootInstrumentationEmitter,
  retry,
  cluster,
}) => {
  const logger = rootLogger.namespace('Admin')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()

  /**
   * @returns {Promise}
   */
  const connect = async () => {
    await cluster.connect()
    instrumentationEmitter.emit(CONNECT)
  }

  /**
   * @return {Promise}
   */
  const disconnect = async () => {
    await cluster.disconnect()
    instrumentationEmitter.emit(DISCONNECT)
  }

  /**
   * @return {Promise}
   */
  const listTopics = async () => {
    const { topicMetadata } = await cluster.metadata()
    const topics = topicMetadata.map(t => t.topic)
    return topics
  }

  /**
   * @param {array} topics
   * @param {boolean} [validateOnly=false]
   * @param {number} [timeout=5000]
   * @param {boolean} [waitForLeaders=true]
   * @return {Promise}
   */
  const createTopics = async ({ topics, validateOnly, timeout, waitForLeaders = true }) => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, the topic names have to be a valid string'
      )
    }

    const topicNames = new Set(topics.map(({ topic }) => topic))
    if (topicNames.size < topics.length) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        await broker.createTopics({ topics, validateOnly, timeout })

        if (waitForLeaders) {
          const topicNamesArray = Array.from(topicNames.values())
          await retryOnLeaderNotAvailable(async () => await broker.metadata(topicNamesArray), {
            delay: 100,
            maxWait: timeout,
            timeoutMessage: 'Timed out while waiting for topic leaders',
          })
        }

        return true
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        if (e.type === 'TOPIC_ALREADY_EXISTS') {
          return false
        }

        bail(e)
      }
    })
  }
  /**
   * @param {array} topicPartitions
   * @param {boolean} [validateOnly=false]
   * @param {number} [timeout=5000]
   * @return {Promise<void>}
   */
  const createPartitions = async ({ topicPartitions, validateOnly, timeout }) => {
    if (!topicPartitions || !Array.isArray(topicPartitions)) {
      throw new KafkaJSNonRetriableError(`Invalid topic partitions array ${topicPartitions}`)
    }
    if (topicPartitions.length === 0) {
      throw new KafkaJSNonRetriableError(`Empty topic partitions array`)
    }

    if (topicPartitions.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topic partitions array, the topic names have to be a valid string'
      )
    }

    const topicNames = new Set(topicPartitions.map(({ topic }) => topic))
    if (topicNames.size < topicPartitions.length) {
      throw new KafkaJSNonRetriableError(
        'Invalid topic partitions array, it cannot have multiple entries for the same topic'
      )
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        await broker.createPartitions({ topicPartitions, validateOnly, timeout })
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string[]} topics
   * @param {number} [timeout=5000]
   * @return {Promise}
   */
  const deleteTopics = async ({ topics, timeout }) => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter(topic => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError('Invalid topics array, the names must be a valid string')
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        await broker.deleteTopics({ topics, timeout })

        // Remove deleted topics
        for (const topic of topics) {
          cluster.targetTopics.delete(topic)
        }

        await cluster.refreshMetadata()
      } catch (e) {
        if (['NOT_CONTROLLER', 'UNKNOWN_TOPIC_OR_PARTITION'].includes(e.type)) {
          logger.warn('Could not delete topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        if (e.type === 'REQUEST_TIMED_OUT') {
          logger.error(
            'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
            {
              error: e.message,
              retryCount,
              retryTime,
            }
          )
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string} topic
   */

  const fetchTopicOffsets = async topic => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.addTargetTopic(topic)
        await cluster.refreshMetadataIfNecessary()

        const metadata = cluster.findTopicPartitionMetadata(topic)
        const high = await cluster.fetchTopicsOffset([
          {
            topic,
            fromBeginning: false,
            partitions: metadata.map(p => ({ partition: p.partitionId })),
          },
        ])

        const low = await cluster.fetchTopicsOffset([
          {
            topic,
            fromBeginning: true,
            partitions: metadata.map(p => ({ partition: p.partitionId })),
          },
        ])

        const { partitions: highPartitions } = high.pop()
        const { partitions: lowPartitions } = low.pop()
        return highPartitions.map(({ partition, offset }) => ({
          partition,
          offset,
          high: offset,
          low: lowPartitions.find(({ partition: lowPartition }) => lowPartition === partition)
            .offset,
        }))
      } catch (e) {
        if (e.type === 'UNKNOWN_TOPIC_OR_PARTITION') {
          await cluster.refreshMetadata()
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string} topic
   * @param {number} [timestamp]
   */

  const fetchTopicOffsetsByTimestamp = async (topic, timestamp) => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.addTargetTopic(topic)
        await cluster.refreshMetadataIfNecessary()

        const metadata = cluster.findTopicPartitionMetadata(topic)
        const partitions = metadata.map(p => ({ partition: p.partitionId }))

        const high = await cluster.fetchTopicsOffset([
          {
            topic,
            fromBeginning: false,
            partitions,
          },
        ])
        const { partitions: highPartitions } = high.pop()

        const offsets = await cluster.fetchTopicsOffset([
          {
            topic,
            fromTimestamp: timestamp,
            partitions,
          },
        ])
        const { partitions: lowPartitions } = offsets.pop()

        return lowPartitions.map(({ partition, offset }) => ({
          partition,
          offset:
            parseInt(offset, 10) >= 0
              ? offset
              : highPartitions.find(({ partition: highPartition }) => highPartition === partition)
                  .offset,
        }))
      } catch (e) {
        if (e.type === 'UNKNOWN_TOPIC_OR_PARTITION') {
          await cluster.refreshMetadata()
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string} groupId
   * @param {string} topic
   * @return {Promise}
   */
  const fetchOffsets = async ({ groupId, topic }) => {
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const partitions = await findTopicPartitions(cluster, topic)
    const coordinator = await cluster.findGroupCoordinator({ groupId })
    const partitionsToFetch = partitions.map(partition => ({ partition }))

    const { responses } = await coordinator.offsetFetch({
      groupId,
      topics: [{ topic, partitions: partitionsToFetch }],
    })

    return responses
      .filter(response => response.topic === topic)
      .map(({ partitions }) =>
        partitions.map(({ partition, offset, metadata }) => ({
          partition,
          offset,
          metadata: metadata || null,
        }))
      )
      .pop()
  }

  /**
   * @param {string} groupId
   * @param {string} topic
   * @param {boolean} [earliest=false]
   * @return {Promise}
   */
  const resetOffsets = async ({ groupId, topic, earliest = false }) => {
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const partitions = await findTopicPartitions(cluster, topic)
    const partitionsToSeek = partitions.map(partition => ({
      partition,
      offset: cluster.defaultOffset({ fromBeginning: earliest }),
    }))

    return setOffsets({ groupId, topic, partitions: partitionsToSeek })
  }

  /**
   * @param {string} groupId
   * @param {string} topic
   * @param {Array<SeekEntry>} partitions
   * @return {Promise}
   *
   * @typedef {Object} SeekEntry
   * @property {number} partition
   * @property {string} offset
   */
  const setOffsets = async ({ groupId, topic, partitions }) => {
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaJSNonRetriableError(`Invalid partitions`)
    }

    const consumer = createConsumer({
      logger: rootLogger.namespace('Admin', LEVELS.NOTHING),
      cluster,
      groupId,
    })

    await consumer.subscribe({ topic, fromBeginning: true })
    const description = await consumer.describeGroup()

    if (!isConsumerGroupRunning(description)) {
      throw new KafkaJSNonRetriableError(
        `The consumer group must have no running instances, current state: ${description.state}`
      )
    }

    return new Promise((resolve, reject) => {
      consumer.on(consumer.events.FETCH, async () =>
        consumer
          .stop()
          .then(resolve)
          .catch(reject)
      )

      consumer
        .run({
          eachBatchAutoResolve: false,
          eachBatch: async () => true,
        })
        .catch(reject)

      // This consumer doesn't need to consume any data
      consumer.pause([{ topic }])

      for (const seekData of partitions) {
        consumer.seek({ topic, ...seekData })
      }
    })
  }

  /**
   * @param {Array<ResourceConfigQuery>} resources
   * @param {boolean} [includeSynonyms=false]
   * @return {Promise}
   *
   * @typedef {Object} ResourceConfigQuery
   * @property {ResourceType} type
   * @property {string} name
   * @property {Array<String>} [configNames=[]]
   */
  const describeConfigs = async ({ resources, includeSynonyms }) => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaJSNonRetriableError(`Invalid resources array ${resources}`)
    }

    if (resources.length === 0) {
      throw new KafkaJSNonRetriableError('Resources array cannot be empty')
    }

    const validResourceTypes = Object.values(RESOURCE_TYPES)
    const invalidType = resources.find(r => !validResourceTypes.includes(r.type))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`
      )
    }

    const invalidName = resources.find(r => !r.name || typeof r.name !== 'string')

    if (invalidName) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`
      )
    }

    const invalidConfigs = resources.find(
      r => !Array.isArray(r.configNames) && r.configNames != null
    )

    if (invalidConfigs) {
      const { configNames } = invalidConfigs
      throw new KafkaJSNonRetriableError(
        `Invalid resource configNames ${configNames}: ${JSON.stringify(invalidConfigs)}`
      )
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        const response = await broker.describeConfigs({ resources, includeSynonyms })
        return response
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not describe configs', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {Array<ResourceConfig>} resources
   * @param {boolean} [validateOnly=false]
   * @return {Promise}
   *
   * @typedef {Object} ResourceConfig
   * @property {ResourceType} type
   * @property {string} name
   * @property {Array<ResourceConfigEntry>} configEntries
   *
   * @typedef {Object} ResourceConfigEntry
   * @property {string} name
   * @property {string} value
   */
  const alterConfigs = async ({ resources, validateOnly }) => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaJSNonRetriableError(`Invalid resources array ${resources}`)
    }

    if (resources.length === 0) {
      throw new KafkaJSNonRetriableError('Resources array cannot be empty')
    }

    const validResourceTypes = Object.values(RESOURCE_TYPES)
    const invalidType = resources.find(r => !validResourceTypes.includes(r.type))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`
      )
    }

    const invalidName = resources.find(r => !r.name || typeof r.name !== 'string')

    if (invalidName) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`
      )
    }

    const invalidConfigs = resources.find(r => !Array.isArray(r.configEntries))

    if (invalidConfigs) {
      const { configEntries } = invalidConfigs
      throw new KafkaJSNonRetriableError(
        `Invalid resource configEntries ${configEntries}: ${JSON.stringify(invalidConfigs)}`
      )
    }

    const invalidConfigValue = resources.find(r =>
      r.configEntries.some(e => typeof e.name !== 'string' || typeof e.value !== 'string')
    )

    if (invalidConfigValue) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource config value: ${JSON.stringify(invalidConfigValue)}`
      )
    }

    const retrier = createRetry(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata()
        const broker = await cluster.findControllerBroker()
        const response = await broker.alterConfigs({ resources, validateOnly: !!validateOnly })
        return response
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not alter configs', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @deprecated - This method was replaced by `fetchTopicMetadata`. This implementation
   * is limited by the topics in the target group, so it can't fetch all topics when
   * necessary.
   *
   * Fetch metadata for provided topics.
   *
   * If no topics are provided fetch metadata for all topics of which we are aware.
   * @see https://kafka.apache.org/protocol#The_Messages_Metadata
   *
   * @param {Object} [options]
   * @param {string[]} [options.topics]
   * @return {Promise<TopicsMetadata>}
   *
   * @typedef {Object} TopicsMetadata
   * @property {Array<TopicMetadata>} topics
   *
   * @typedef {Object} TopicMetadata
   * @property {String} name
   * @property {Array<PartitionMetadata>} partitions
   *
   * @typedef {Object} PartitionMetadata
   * @property {number} partitionErrorCode Response error code
   * @property {number} partitionId Topic partition id
   * @property {number} leader  The id of the broker acting as leader for this partition.
   * @property {Array<number>} replicas The set of all nodes that host this partition.
   * @property {Array<number>} isr The set of nodes that are in sync with the leader for this partition.
   */
  const getTopicMetadata = async options => {
    const { topics } = options || {}

    if (topics) {
      await Promise.all(
        topics.map(async topic => {
          if (!topic) {
            throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
          }

          try {
            await cluster.addTargetTopic(topic)
          } catch (e) {
            e.message = `Failed to add target topic ${topic}: ${e.message}`
            throw e
          }
        })
      )
    }

    await cluster.refreshMetadataIfNecessary()
    const targetTopics = topics || [...cluster.targetTopics]

    return {
      topics: await Promise.all(
        targetTopics.map(async topic => ({
          name: topic,
          partitions: cluster.findTopicPartitionMetadata(topic),
        }))
      ),
    }
  }

  /**
   * Fetch metadata for provided topics.
   *
   * If no topics are provided fetch metadata for all topics.
   * @see https://kafka.apache.org/protocol#The_Messages_Metadata
   *
   * @param {Object} [options]
   * @param {string[]} [options.topics]
   * @return {Promise<TopicsMetadata>}
   *
   * @typedef {Object} TopicsMetadata
   * @property {Array<TopicMetadata>} topics
   *
   * @typedef {Object} TopicMetadata
   * @property {String} name
   * @property {Array<PartitionMetadata>} partitions
   *
   * @typedef {Object} PartitionMetadata
   * @property {number} partitionErrorCode Response error code
   * @property {number} partitionId Topic partition id
   * @property {number} leader  The id of the broker acting as leader for this partition.
   * @property {Array<number>} replicas The set of all nodes that host this partition.
   * @property {Array<number>} isr The set of nodes that are in sync with the leader for this partition.
   */
  const fetchTopicMetadata = async ({ topics = [] } = {}) => {
    if (topics) {
      topics.forEach(topic => {
        if (!topic || typeof topic !== 'string') {
          throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
        }
      })
    }

    const metadata = await cluster.metadata({ topics })

    return {
      topics: metadata.topicMetadata.map(topicMetadata => ({
        name: topicMetadata.topic,
        partitions: topicMetadata.partitionMetadata,
      })),
    }
  }

  /**
   * Describe cluster
   *
   * @return {Promise<ClusterMetadata>}
   *
   * @typedef {Object} ClusterMetadata
   * @property {Array<Broker>} brokers
   * @property {Number} controller Current controller id. Returns null if unknown.
   * @property {String} clusterId
   *
   * @typedef {Object} Broker
   * @property {Number} nodeId
   * @property {String} host
   * @property {Number} port
   */
  const describeCluster = async () => {
    const { brokers: nodes, clusterId, controllerId } = await cluster.metadata({ topics: [] })
    const brokers = nodes.map(({ nodeId, host, port }) => ({
      nodeId,
      host,
      port,
    }))
    const controller =
      controllerId == null || controllerId === NO_CONTROLLER_ID ? null : controllerId

    return {
      brokers,
      controller,
      clusterId,
    }
  }

  /**
   * List groups in a broker
   *
   * @return {Promise<ListGroups>}
   *
   * @typedef {Object} ListGroups
   * @property {Array<ListGroup>} groups
   *
   * @typedef {Object} ListGroup
   * @property {string} groupId
   * @property {string} protocolType
   */
  const listGroups = async () => {
    await cluster.refreshMetadata()
    let groups = []
    for (var nodeId in cluster.brokerPool.brokers) {
      const broker = await cluster.findBroker({ nodeId })
      const response = await broker.listGroups()
      groups = groups.concat(response.groups)
    }

    return { groups }
  }

  /**
   * Describe groups by group ids
   * @param {Array<string>} groupIds
   *
   * @typedef {Object} GroupDescriptions
   * @property {Array<GroupDescription>} groups
   *
   * @return {Promise<GroupDescriptions>}
   */
  const describeGroups = async groupIds => {
    const coordinatorsForGroup = await Promise.all(
      groupIds.map(async groupId => {
        const coordinator = await cluster.findGroupCoordinator({ groupId })
        return {
          coordinator,
          groupId,
        }
      })
    )

    const groupsByCoordinator = Object.values(
      coordinatorsForGroup.reduce((coordinators, { coordinator, groupId }) => {
        const group = coordinators[coordinator.nodeId]

        if (group) {
          coordinators[coordinator.nodeId] = {
            ...group,
            groupIds: [...group.groupIds, groupId],
          }
        } else {
          coordinators[coordinator.nodeId] = { coordinator, groupIds: [groupId] }
        }
        return coordinators
      }, {})
    )

    const responses = await Promise.all(
      groupsByCoordinator.map(async ({ coordinator, groupIds }) => {
        const retrier = createRetry(retry)
        const { groups } = await retrier(() => coordinator.describeGroups({ groupIds }))
        return groups
      })
    )

    const groups = [].concat.apply([], responses)

    return { groups }
  }

  /**
   * Delete groups in a broker
   *
   * @param {string[]} [groupIds]
   * @return {Promise<DeleteGroups>}
   *
   * @typedef {Array} DeleteGroups
   * @property {string} groupId
   * @property {number} errorCode
   */
  const deleteGroups = async groupIds => {
    if (!groupIds || !Array.isArray(groupIds)) {
      throw new KafkaJSNonRetriableError(`Invalid groupIds array ${groupIds}`)
    }

    const invalidGroupId = groupIds.some(g => typeof g !== 'string')

    if (invalidGroupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`)
    }

    const retrier = createRetry(retry)

    let results = []

    let clonedGroupIds = groupIds.slice()

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        if (clonedGroupIds.length === 0) return []

        await cluster.refreshMetadata()

        const brokersPerGroups = {}
        const brokersPerNode = {}
        for (const groupId of clonedGroupIds) {
          const broker = await cluster.findGroupCoordinator({ groupId })
          if (brokersPerGroups[broker.nodeId] === undefined) brokersPerGroups[broker.nodeId] = []
          brokersPerGroups[broker.nodeId].push(groupId)
          brokersPerNode[broker.nodeId] = broker
        }

        const res = await Promise.all(
          Object.keys(brokersPerNode).map(
            async nodeId => await brokersPerNode[nodeId].deleteGroups(brokersPerGroups[nodeId])
          )
        )

        const errors = flatten(
          res.map(({ results }) =>
            results.map(({ groupId, errorCode, error }) => {
              return { groupId, errorCode, error }
            })
          )
        ).filter(({ errorCode }) => errorCode !== 0)

        clonedGroupIds = errors.map(({ groupId }) => groupId)

        if (errors.length > 0) throw new KafkaJSDeleteGroupsError('Error in DeleteGroups', errors)

        results = flatten(res.map(({ results }) => results))

        return results
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER' || e.type === 'COORDINATOR_NOT_AVAILABLE') {
          logger.warn('Could not delete groups', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string} eventName
   * @param {Function} listener
   * @return {Function}
   */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(unwrapEvent(eventName), event => {
      event.type = wrapEvent(event.type)
      Promise.resolve(listener(event)).catch(e => {
        logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
        })
      })
    })
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  return {
    connect,
    disconnect,
    listTopics,
    createTopics,
    deleteTopics,
    createPartitions,
    getTopicMetadata,
    fetchTopicMetadata,
    describeCluster,
    events,
    fetchOffsets,
    fetchTopicOffsets,
    fetchTopicOffsetsByTimestamp,
    setOffsets,
    resetOffsets,
    describeConfigs,
    alterConfigs,
    on,
    logger: getLogger,
    listGroups,
    describeGroups,
    deleteGroups,
  }
}
