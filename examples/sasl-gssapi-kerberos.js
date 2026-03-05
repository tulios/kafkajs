/**
 * Example: SASL GSSAPI (Kerberos) Authentication
 *
 * Uses Kerberos tickets for authentication. This mechanism provides
 * strong mutual authentication without transmitting passwords.
 *
 * Prerequisites:
 *   1. Install the kerberos npm package: npm install kerberos
 *   2. A Kerberos KDC (Key Distribution Center) must be running
 *   3. A keytab file or valid TGT must be available
 *   4. Proper Kerberos configuration (/etc/krb5.conf)
 *
 * Broker configuration (server.properties):
 *   sasl.enabled.mechanisms=GSSAPI
 *   sasl.kerberos.service.name=kafka
 *
 * Usage:
 *   KERBEROS_SERVICE_NAME=kafka \
 *   KERBEROS_PRINCIPAL=kafka-client@EXAMPLE.COM \
 *   KERBEROS_KEYTAB=/etc/security/keytabs/client.keytab \
 *   KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
 *   node examples/sasl-gssapi-kerberos.js
 */

const { Kafka, logLevel } = require('../index')

const kafka = new Kafka({
  clientId: 'sasl-gssapi-example',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.INFO,
  ssl: process.env.SSL === 'true' || undefined,
  sasl: {
    mechanism: 'gssapi',
    // The Kerberos service name (default: 'kafka')
    serviceName: process.env.KERBEROS_SERVICE_NAME || 'kafka',
    // The client principal (e.g., 'kafka-client@EXAMPLE.COM')
    principal: process.env.KERBEROS_PRINCIPAL || undefined,
    // Path to the keytab file
    keytab: process.env.KERBEROS_KEYTAB || undefined,
    // Optional: override the full service principal (e.g., 'kafka/broker-host@REALM')
    // If not set, it is constructed as serviceName/host
    kerberosServicePrincipal: process.env.KERBEROS_SERVICE_PRINCIPAL || undefined,
  },
})

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  console.log('Connected with SASL GSSAPI (Kerberos) authentication')

  const topics = await admin.listTopics()
  console.log('Topics:', topics)

  await admin.disconnect()
}

run().catch(e => {
  console.error(`Error: ${e.message}`, e)
  process.exit(1)
})
