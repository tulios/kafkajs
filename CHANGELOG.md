# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [1.4.0] - 2018-10-09

### Fixed
  - Potential offset loss when updating offsets for resolved partitions #124
  - Refresh metadata on lock timeout #131
  - Cleans up stale brokers on metadata refresh #131
  - Force metadata refresh on `ECONNREFUSED` #134
  - Handle API version not supported #135
  - Handle v0.10 messages on v0.11 Fetch API #143

### Added
  - Admin delete topics #117
  - Update metadata api and allow to disable auto topic creation #118
  - Use highest available API version #135 #146
  - Admin describe and alter configs #138
  - Validate message format in producer #142
  - Consumers can detect that a topic was updated and force a rebalance #136

### Changed
  - Improved stack trace for `KafkaJSNumberOfRetriesExceeded` #123
  - Enable Kafka v0.11 API by default #141 (Can still be disabled with `allowExperimentalV011=false`)
  - Replace event emitter Lock #154
  - Add member assignment to `GROUP_JOIN` instrumentation event #136

## [1.3.1] - 2018-08-20
### Fixed
  - Client logger accessor #106
  - Producer v3 decode format #114
  - Parsing multiple responses #115
  - Fetch v4 for partial messages on record batch #116

### Added
  - Connection instrumentation events #110

## [1.3.0] - 2018-08-06
### Fixed
  - Skip unsubscribed topic assignment #86
  - Refresh metadata when producing to a topic without metadata #87
  - Discard messages with a lower offset than requested #100

### Added
  - Add consumer auto commit policies #89
  - Notify user when setting heartbeat interval to same or higher than session timeout #91
  - Constantly refresh metadata based on `metadataMaxAge` #94
  - New instrumentation events #95
  - Expose loggers #97 #102
  - Add offset management operations to the admin client #101
  - Support to record batch compression #103
  - Handle missing username/password during authentication #104

## [1.2.0] - 2018-07-02
### Fixed
  - Make sure authentication handshake remains consistent event when `broker.connect` is called concurrently #81

### Added
  - Add `producer.sendBatch` to produce to multiple topics at once #82
  - Experimental support to native Kafka 0.11 (`allowExperimentalV011`) #61 #68 #75 #76 #78
  - Enabled protocol `fetch` v3

## [1.1.0] - 2018-06-14
### Added
  - Support to SASL SCRAM (`scram-sha-256` and `scram-sha-512`) #72
  - Admin client with support to create topics #73

## [1.0.1] - 2018-05-18
### Fixed
  - Prevent crash when re-producing after metadata refresh #62

## [1.0.0] - 2018-05-14
### Changed
  - Updated readme

## [0.8.1] - 2018-04-10
### Fixed
  - Throw unretriable error for incompatible message format #49
  - Producer can't reconnect after broker disconnection #48

## [0.8.0] - 2018-04-05
### Removed
  - Backwards compatibility with the old member assignment protocol (<= 0.6.x). __v0.7.x__ is the last version with support for both protocols (commit f965e91cf883bff74332e53a8c1d25c2af39e566)

### Fixed
  - Only retry failed brokers #38

### Changed
  - Use selected assigner on consumer sync #35
  - Add validations to consumer subscribe and producer send #41

### Added
  - Consumer pause and resume #41
  - Allow `partitionAssigners` to be configured
  - Allow auto-resolve to be disabled for eachBatch #44

## [0.7.1] - 2018-01-22
### Fixed
  - Fix member assignment protocol #33

## [0.7.0] - 2018-01-19
### Fixed
  - Fix retry on error for message handlers #30

### Changed
  - Use decoder offset for validating response length #21
  - Change log creator to improve interoperability #24
  - Add support to `KAFKAJS_LOG_LEVEL` #24
  - Improved assigner protocol #27 #29

### Added
  - Add seek API to consumer #23
  - Add experimental describe group to consumer #31

## [0.6.8] - 2017-12-27
### Fixed
  - Only use seed brokers to bootstrap consumers (introduced a broker pool abstraction) #19
  - Don't include the size of the size field when determining if the buffer has enough bytes to read the full response #20

## [0.6.7] - 2017-12-18
### Fixed
  - Don't throw KafkaJSOffsetOutOfRange on every protocol error

## [0.6.6] - 2017-12-15
### Fixed
  - Fix index out of range errors when decoding partial messages #11
  - Don't rely on seed broker for finding group coordinator #13

## [0.6.5] - 2017-12-14
### Fixed
  - Fix partial message decode #10
  - Throw not implemented error for Snappy and LZ4 compression #10
  - Don't crash when setting offsets to default #10

## [0.6.4] - 2017-12-13
### Added
  - Add stack trace to connection error logs

### Changed
  - Skip consumer callback error logs for kafkajs errors

### Fixed
  - Use the connection timeout callback for socket timeout
  - Check if still connected before writing to the socket

## [0.6.3] - 2017-12-11
### Added
  - Add error logs for user errors in `eachMessage` and `eachBatch`

### Fixed
  - Recover from rebalance in progress when starting the consumer

## [0.6.2] - 2017-12-11
### Added
  - Add `logger: "kafkajs"` to log lines

## [0.6.1] - 2017-12-08
### Added
  - Add latest Kafka error codes

### Fixed
  - Add fallback error for unsupported error codes

## [0.6.0] - 2017-12-07
### Added
  - Expose consumer group `isRunning` to `eachBatch`
  - Add `offsetLag` to consumer batch

## [0.5.0] - 2017-12-04
### Added
  - Add ability to subscribe to events (heartbeats and offsets commit) #4

## [0.4.1] - 2017-11-30
### Changed
  - Add heartbeat after each message
  - Update default heartbeat interval to a better value

### Fixed
  - Accept all consumer properties from create consumer

## [0.4.0] - 2017-11-23
### Added
  - Commit previously resolved offsets when `eachBatch` throws an error
  - Commit previously resolved offsets when `eachMessage` throws an error
  - Consumer group recover from offset out of range

## [0.3.2] - 2017-11-23
### Fixed
  - Stop consuming messages when the consumer group is not running

## [0.3.1] - 2017-11-20
### Fixed
  - NPM bundle (add .npmignore)
  - Fix package.json reference to main file

## [0.3.0] - 2017-11-20
### Added
  - Add cluster method to fetch offsets for a list of topics
  - Add offset resolution to the consumer group

### Changed
  - Rename protocol Offsets to ListOffsets
  - Update cluster fetch topics offset to allow different configurations per topic
  - Create different instances of the cluster for producers and consumers

### Fixed
  - Fix the use of timestamp in the ListOffsets protocol
  - Fix create topic script
  - Prevent unnecessary reconnections on cluster connect

## [0.2.0] - 2017-11-13
### Added
  - Expose consumer groups
  - Message and MessageSet V0 and V1 decoder
  - Protocol fetch V0, V1, V2 and V3
  - Protocol find coordinator V0
  - Protocol join group V0
  - Protocol sync group V0
  - Protocol leave group V0
  - Protocol offsets V0
  - Protocol offset commit V0, V1 and V2
  - Protocol offset fetch V1 and V2
  - Protocol heartbeat V0
  - Support to compressed message sets in protocol fetch
  - Add find coordinator group to cluster
  - Set timestamp for compressed messages
  - Travis integration
  - Eslint and prettier

### Changed
  - Throw an error when cluster findBroker doesn't find a broker
  - Move `KafkaProtocolError` to errors file
  - Convert encode, decode and parse to async functions
  - Update producer to use async compressor
  - Update fetch to use async decompressor
  - Create a separate error class for SASL errors
  - Accepts a list of brokers instead of host and port
  - Move retry logic out of connection and create namespaces for the logger
  - Retry when refresh metadata throws leader not available

### Fixed
  - Fix broker maxWaitTime default value
  - Take OS differences when asserting gzip results
  - Clear the connection timeout when the connection ends or fails due to an error

## [0.1.2] - 2017-10-17
### Added
  - Expose if the cluster is connected

### Fixed
  - Reconnect the cluster if it is not connected when producing messages
  - Throw error if metadata is still not loaded when finding the topic partition
  - Refresh metadata in case it is not loaded
  - Make connection throw a retriable error when not connected

## [0.1.1] - 2017-10-16
### Changed
  - Only retry retriable errors
  - Propagate clientId and connectionTimeout to Cluster

### Fixed
  - Typo when loading the SASL Handshake protocol

## [0.1.0] - 2017-10-15
### Added
  - Producer compatible with Kafka 0.10.x
  - GZIP compression
  - Plain, SSL and SASL_SSL implementations
  - Published on Github
