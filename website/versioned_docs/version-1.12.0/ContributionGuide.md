---
id: version-1.12.0-contribution-guide
title: Contributing
original_id: contribution-guide
---

KafkaJS is an open-source project where development takes place in the open on GitHub. Although the project is maintained by a small group of dedicated volunteers, we are grateful to the community for bugfixes, feature development and other contributions.

Issues are tracked in [Github](https://github.com/tulios/kafkajs/issues). For first time contributors, we maintain a list of [Good First Issues](https://github.com/tulios/kafkajs/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). If you are planning to implement a new feature or work on fixing a bug, make sure to [check the issue tracker](https://github.com/tulios/kafkajs/issues) to see if someone is already working on it, or [open an issue](https://github.com/tulios/kafkajs/issues/new) before you start your work. [The Slack channel](https://kafkajs-slackin.herokuapp.com/) is also a good place if you want to discuss your plans before starting your implementation.

## TL;DR

The following chapters will get you set up with a working development environment and teach you how to run the tests. If you are already familiar with the project setup, here's the gist:

With `docker`, `docker-compose` and `yarn` available:

```sh
yarn test
# or
./scripts/dockerComposeUp.sh
./scripts/createScramCredentials.sh
yarn test:local:watch
```