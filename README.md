https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
http://kafka.apache.org/protocol.html

## Development

To try the project locally:

```sh
# This will run a kafka cluster configured with your current IP
./scripts/dockerComposeUp.sh
yarn test:local
```

or

```sh
yarn test
```

Password for test keystore and certificates: `testtest`
Password for SASL `test:testtest`
