#!/usr/bin/env node

const execa = require('execa')
const crypto = require('crypto')

const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')

const findContainerId = node => {
  const cmd = `
    docker ps \
      --filter "status=running" \
      --filter "label=com.docker.compose.project=kafkajs" \
      --filter "label=com.docker.compose.service=${node}" \
      --no-trunc \
      -q
  `
  const containerId = execa.shellSync(cmd).stdout.toString('utf-8')
  console.log(`${node}: ${containerId}`)
  return containerId
}

const waitForNode = containerId => {
  const cmd = `
    docker exec \
      ${containerId} \
      bash -c "JMX_PORT=9998 /opt/kafka/bin/kafka-topics.sh --zookeeper zk:2181 --list 2> /dev/null"
    sleep 5
  `

  execa.shellSync(cmd)
  console.log(`Kafka container ${containerId} is running`)
}

const createTopic = (containerId, topicName) => {
  const cmd = `
    docker exec \
      ${containerId} \
      bash -c "JMX_PORT=9998 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic ${topicName} --replication-factor 1 --partitions 2 --zookeeper zk:2181 2> /dev/null"
  `

  return execa.shellSync(cmd).stdout.toString('utf-8')
}

const consumerGroupDescribe = containerId => {
  const cmd = `
    docker exec \
      ${containerId} \
      bash -c "JMX_PORT=9998 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group test-group-${secureRandom()} --describe > /dev/null 2>&1"
    sleep 1
  `
  return execa.shellSync(cmd).stdout.toString('utf-8')
}

console.log('\nFinding container ids...')
const kafka1ContainerId = findContainerId('kafka1')
const kafka2ContainerId = findContainerId('kafka2')
const kafka3ContainerId = findContainerId('kafka3')

console.log('\nWaiting for nodes...')
waitForNode(kafka1ContainerId)
waitForNode(kafka2ContainerId)
waitForNode(kafka3ContainerId)

console.log('\nAll nodes up:')
console.log(
  execa.shellSync(`HOST_IP=${process.env.HOST_IP} docker-compose ps`).stdout.toString('utf-8')
)

console.log('\nCreating default topics...')
createTopic(kafka1ContainerId, 'test-topic-already-exists')

console.log('\nWarming up Kafka...')

const totalRandomTopics = 10
console.log(`  -> creating ${totalRandomTopics} random topics...`)
Array(totalRandomTopics)
  .fill()
  .forEach(() => {
    createTopic(kafka1ContainerId, `test-topic-${secureRandom()}`)
  })

console.log('  -> running consumer describe')
consumerGroupDescribe(kafka1ContainerId)
