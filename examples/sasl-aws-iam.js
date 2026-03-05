/**
 * Example: SASL AWS IAM Authentication
 *
 * Authenticates using AWS IAM credentials. This is commonly used with
 * Amazon MSK (Managed Streaming for Apache Kafka).
 *
 * The mechanism sends AWS access key, secret key, and optional session
 * token to authenticate against the broker.
 *
 * Requires STACK's Kafka AWS IAM LoginModule or a compatible alternative
 * installed on the brokers.
 *
 * For Amazon MSK, see also: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
 *
 * Usage:
 *   AWS_AUTH_IDENTITY=AIDAIOSFODNN7EXAMPLE \
 *   AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
 *   AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
 *   AWS_SESSION_TOKEN=optional-session-token \
 *   KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
 *   node examples/sasl-aws-iam.js
 */

const { Kafka, logLevel } = require('../index')

const kafka = new Kafka({
  clientId: 'sasl-aws-iam-example',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.INFO,

  // SSL is recommended for AWS IAM auth
  ssl: true,

  sasl: {
    mechanism: 'aws',
    // The UserId or RoleId from `aws iam get-user` or `aws iam get-role`
    authorizationIdentity: process.env.AWS_AUTH_IDENTITY || 'AIDAIOSFODNN7EXAMPLE',
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey:
      process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    // Optional: required when using temporary credentials (STS AssumeRole)
    sessionToken: process.env.AWS_SESSION_TOKEN || undefined,
  },
})

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  console.log('Connected with SASL AWS IAM authentication')

  const topics = await admin.listTopics()
  console.log('Topics:', topics)

  await admin.disconnect()
}

run().catch(e => {
  console.error(`Error: ${e.message}`, e)
  process.exit(1)
})
