---
id: version-1.12.0-testing
title: Testing
original_id: testing
---

We rely on a suite of largely integration tests, using `jest` as a test runner.

To run all the tests a single time without having booted any containers, you can run `yarn test`. This will lint the code, boot all the containers, run all the tests and then tear down the containers.

For development, it is usually more convenient to boot the containers yourself, and then keep the tests running in watch mode. After you have booted the containers as described in [Running Kafka](ContributionGuide.md#running-kafka), you can run `yarn test:local:watch`. `jest`'s interactive mode will allow you select which tests to run.

## Logging

Logs are usually disabled during test runs. To enable logging, override the log level with the environment variable `KAFKAJS_LOG_LEVEL`.

In order to avoid overloading the terminal with huge logs, we filter any buffers that would otherwise be printed from the protocol debug logs. If you need to see the actual buffer values in your logs, override this with the environment variable `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_PROTOCOL_BUFFERS=1 yarn test:local:watch
```

Because the `Fetch` response log can be extremely large, we always filter the `Fetch` response payload, unless the `KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS` environment variable is *also*  set. To see the full `Fetch` response buffer values, run with both flags set:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_PROTOCOL_BUFFERS=1 KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS=1 yarn test:local:watch
```

## Helpers

[`testHelpers/index.js`](https://github.com/tulios/kafkajs/blob/master/testHelpers/index.js) provides several helpers for connecting to your cluster and asserting upon its state. Tests are typically co-located with the code that they are testing. See [`src/consumer/__tests__/consumeMessages.spec.js`](https://github.com/tulios/kafkajs/blob/master/src/consumer/__tests__/consumeMessages.spec.js) for an example that makes use of the helpers to implement an integration test.