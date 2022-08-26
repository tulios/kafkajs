---
id: faq
title: FAQ
---

## Is KafkaJS used in production?

**Yes**. KafkaJS is used across hundreds of services running in production across businesses over the world.

> **Note**: If you are using KafkaJS in production, [we would love to hear about what you are doing!](https://github.com/tulios/kafkajs/issues/289)

## Why am I receiving messages for topics I'm not subscribed to?

If you are seeing the warning `[ConsumerGroup] Consumer group received unsubscribed topics`, it likely means that some members of your consumer group are subscribed to some topics, and some other members of the group are subscribed to a different set of topics. In our experience, the most common cause is re-using `groupId` across several applications or several different independent deployments of the same application. This is normal while deploying a new version of an application where the new version subscribes to a new topic, and the warning will go away once the group stabilizes on a single version.

Ensure that your `groupId` is not used by any other application. A simple way to verify this is to [describe the consumer group](Consuming.md#describe-group) and verify that there are no unexpected members.

Note: If you are switching from node-rdkafka, then this behavior is not the same. You may want to listen to the [RECEIVED_UNSUBSCRIBED_TOPICS](InstrumentationEvents.md#a-name-list-list-of-available-events) event and act accordingly.

## What does it mean to get `REBALANCE_IN_PROGRESS` errors?

This is a normal occurrence during, for example, deployments, and should resolve itself. However, if this continues to happen frequently when the group should be stable, it may indicate that your session timeout is too low, or that processing each message is taking too long.

Every instance of your application is a member of a consumer group, and is exclusively assigned one or more partitions to consume from. This means that that consumer is the only one within the consumer group that is allowed to consume from that partition. A rebalance means that this ownership is being re-assigned.

A rebalance will happen in a number of scenarios:

* A new member joins the consumer group
* A member leaves the consumer group (for example when shutting down)
* A member is considered dead by the group coordinator. This happens when there have been no heartbeats sent within the configured session timeout. This would indicate that the consumer has crashed or is busy with some long-running processing, such as for example if the execution of `eachMessage` takes longer than the session timeout.
* Partitions have been added or removed from the topic

The rebalancing state is enforced on the broker side. When a consumer tries to commit offsets, the broker will respond with `REBALANCE_IN_PROGRESS`. Upon receiving that, the consumer group leader will receive a list of currently active members of the consumer group. Using the [partition assigner](Consuming.md#a-name-custom-partition-assigner-a-custom-partition-assigner) configured on the client, the consumer group leader will assign each partition to a consumer within the group and submit the assignment back to the group coordinator, which distributes the relevant assignments to the members of the consumer group. When this is done, the group is considered in-sync again and processing can continue.

## Can KafkaJS be used in a browser?

No - KafkaJS is a library for NodeJS and will not work in a browser context.

Communication with Kafka happens over a TCP socket. The NodeJS API used to create that socket is [`net.connect`](https://nodejs.org/api/net.html#net_net_connect)/[`tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback). There is no equivalent API in browsers [as of yet](https://github.com/WICG/raw-sockets), which means that even if you run KafkaJS through a transpilation tool like [Browserify](http://browserify.org/), it cannot polyfill those modules.

For more information, see:

* [#508](https://github.com/tulios/kafkajs/issues/508#issuecomment-535382981)
* [#36](https://github.com/tulios/kafkajs/issues/36)
* [Custom Socket Factory](https://kafka.js.org/docs/configuration#custom-socket-factory)

## Didn't find what you were looking for?

Please [open an issue](https://github.com/tulios/kafkajs/issues) or [join our Slack community](https://join.slack.com/t/kafkajs/shared_invite/zt-1ezd5395v-SOpTqYoYfRCyPKOkUggK0A)
