# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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
