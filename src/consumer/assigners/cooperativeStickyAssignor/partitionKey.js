/**
 * Canonical string key for a { topic, partition } pair, used for set/map
 * membership. The separator is collision-safe because Kafka topic names cannot
 * contain ':' (only [a-zA-Z0-9._-]).
 *
 * @param {{ topic: string, partition: number }} topicPartition
 * @returns {string}
 */
const partitionKey = ({ topic, partition }) => topic + ':' + partition

module.exports = { partitionKey }
