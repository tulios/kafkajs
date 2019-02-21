---
id: faq
title: Frequently Asked Questions
---

## Is KafkaJS used in production?

**Yes**. KafkaJS is used across hundreds of services running in production across businesses over the world.

> **Note**: If you are using KafkaJS in production, [we would love to hear about what you are doing!](https://github.com/tulios/kafkajs/issues/289)

## Why am I receiving messages for topics I'm not subscribed to?

If you are seeing the warning `[ConsumerGroup] Consumer group received unsubscribed topics`, it likely means that some members of your consumer group are subscribed to some topics, and some other members of the group are subscribed to a different set of topics. In our experience, the most common cause is re-using `groupId` across several applications or several different independent deployments of the same application. This is normal while deploying a new version of an application where the new version subscribes to a new topic, and the warning will go away once the group stabilizes on a single version.

Ensure that your `groupId` is not used by any other application. A simple way to verify this is to [describe the consumer group](Consuming.md#describe-group) and verify that there are no unexpected members.

## Didn't find what you were looking for?

Please [open an issue](https://github.com/tulios/kafkajs/issues) or [join our Slack community](https://kafkajs-slackin.herokuapp.com)