// From:
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java#L31

module.exports = {
  /**
   * Represents any ResourceType which this client cannot understand,
   * perhaps because this client is too old.
   */
  UNKNOWN: 0,
  /**
   * In a filter, matches any ResourceType.
   */
  ANY: 1,
  /**
   * A Kafka topic.
   * @see http://kafka.apache.org/documentation/#topicconfigs
   */
  TOPIC: 2,
  /**
   * A consumer group.
   * @see http://kafka.apache.org/documentation/#consumerconfigs
   */
  GROUP: 3,
  /**
   * The cluster as a whole.
   */
  CLUSTER: 4,
  /**
   * A transactional ID.
   */
  TRANSACTIONAL_ID: 5,
  /**
   * A token ID.
   */
  DELEGATION_TOKEN: 6,
}
