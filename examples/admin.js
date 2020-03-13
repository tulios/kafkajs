const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('../index')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'test-admin-id',
  ssl: {
    servername: 'localhost',
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
})

const topic1 = 'topic-test1'
const topic2 = 'topic-test2'
const topic3 = 'topic-test3'

const expectedTopics = [topic1, topic2, topic3]

const filterTopics = ({ name }) => expectedTopics.includes(name)
const sortTopics = (a, b) => {
  if (a.name < b.name) {
    return -1
  }
  if (a.name > b.name) {
    return 1
  }
  return 0
}

const admin = kafka.admin()

const getMyTopics = async () => {
  const md = await admin.fetchTopicMetadata({})
  return md.topics.filter(filterTopics).sort(sortTopics)
}

const wait = nrOfSec => {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, nrOfSec * 1000)
  })
}

const run = async () => {
  await admin.connect()
  // Read back topic to check if we need to delete topics
  let myTopics = await getMyTopics()
  // delete test topic, ignore error
  if (myTopics.length > 0) {
    const topicNames = myTopics.map(({ name }) => name)
    console.log('We have topics to delete: %s', topicNames.join(', '))
    try {
      await admin.deleteTopics({ topics: topicNames })
    } catch (err) {
      console.log('Error deleting topic: ', err)
    }
    console.log('Wait for delete to take effect')
    await wait(5)
  }
  // We need to wait 10 seconds to make sure topics get deleted
  myTopics = await getMyTopics()
  if (myTopics.length !== 0) {
    console.log('We should not have any topics here? %j', myTopics)
  } else {
    console.log('We have now 0 topics')
  }
  console.log('(Re)creating topics')
  try {
    console.log('Creating topics')
    await admin.createTopics({
      topics: expectedTopics.map(topic => ({ topic })),
      waitForLeaders: true,
    })
  } catch (err) {
    console.log('Error creating topics: ', err)
  }
  // Validate that we have our topics
  myTopics = await getMyTopics()
  if (myTopics.length !== expectedTopics.length) {
    console.log('We are missing the %s topics: %j', expectedTopics.length, myTopics)
  }
  console.log('Add new partitions')
  try {
    await admin.createPartitions({
      topicPartitions: [
        {
          topic: topic1,
          count: 3, // test with no assignments
        },
        {
          topic: topic2,
          count: 4,
          // test with correct assignments
          assignments: [[1], [0], [2]],
        },
        {
          topic: topic3,
          count: 5,
          // test with one of the assignment null
          assignments: [[1], [0], [2], [1]],
        },
      ],
    })
  } catch (err) {
    console.log('Error: ', err)
    throw err
  }
  console.log('Wait for new partitions take effect')
  await wait(5)
  // Read now information back to check
  myTopics = await getMyTopics()
  console.log(
    'Topics: %s',
    myTopics.map(t => `${t.name} => ${t.partitions.length} part`).join(', ')
  )
  await admin.disconnect()
}

run().catch(e => console.error(`[example/admin] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await admin.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await admin.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
