---
id: migration-guide-v3.0.0
title: Migration Guide v3.0.0
sidebar_label: v3.0.0
---

## Overview

KafkaJS v3.0.0 adds full support for Apache Kafka 4.2.0 with KRaft-native architecture. This is a feature release with no breaking changes for existing producer/consumer code.

## What's New

### Kafka 4.2.0 Compatibility

KafkaJS automatically negotiates protocol versions with Kafka 4.2.0 brokers. No configuration changes needed.

### Share Groups (KIP-932)

Share Groups provide queue-like consumption for Kafka 4.0+. Full protocol support is included for:

- **ShareGroupHeartbeat** - Member lifecycle management
- **ShareGroupDescribe** - Inspect share group state and assignments
- **ShareFetch** - Fetch records with per-record acknowledgement tracking
- **ShareAcknowledge** - Accept, release, or reject individual records

```javascript
// Admin: describe share groups
const result = await admin.shareGroupDescribe({
  groupIds: ['my-share-group'],
})
```

### KRaft Voter Management

Dynamic voter management for KRaft clusters (Kafka 4.1+):

- **AddRaftVoter** - Add a voter to the KRaft quorum
- **RemoveRaftVoter** - Remove a voter from the KRaft quorum
- **UpdateRaftVoter** - Update voter endpoints or supported versions

### Protocol Version Upgrades

Newer API versions with flexible encoding (KIP-482) for all major APIs. See the [Upgrade Guide](https://github.com/tulios/kafkajs/blob/master/UPGRADE.md#protocol-version-upgrades) for the full version table.

### IncrementalAlterConfigs

New admin method for incremental config changes (SET, DELETE, APPEND, SUBTRACT):

```javascript
const { ConfigResourceTypes, ConfigOperationTypes } = require('kafkajs')

await admin.incrementalAlterConfigs({
  resources: [{
    type: ConfigResourceTypes.TOPIC,
    name: 'my-topic',
    configEntries: [
      { name: 'cleanup.policy', configOperation: ConfigOperationTypes.SET, value: 'compact' },
    ],
  }],
})
```

### New Admin Methods

- `electLeaders()` - Trigger preferred or unclean leader election
- `deleteOffsets()` - Delete consumer group offsets
- `describeLogDirs()` - Inspect broker log directories
- `describeProducers()` - List active producers per partition
- `describeTransactions()` - Inspect active transactions
- `listTransactions()` - List transactions with state/producer filters
- `shareGroupDescribe()` - Describe share groups (Kafka 4.0+)

### New Error Codes

All error codes through Kafka 4.2.0 (codes 89-127) including Share Group errors.

### UUID Wire Type

Encoder/decoder support for 128-bit UUID fields, used by Share Group and KRaft APIs for topic and voter identifiers.

### Node.js 20+ Required

KafkaJS v3.0.0 requires Node.js >= 20.0.0 and npm >= 10.0.0. Node 14/16/18 are no longer supported.

## Upgrading

```bash
npm install kafkajs@3.0.0
```

Ensure you are running Node.js 20 or later:

```bash
node --version  # must be >= v20.0.0
```

No code changes required for existing producer/consumer usage. The client automatically negotiates compatible protocol versions with your Kafka brokers.
