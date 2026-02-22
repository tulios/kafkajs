/**
 * Cooperative Sticky Assignor - end-to-end demo
 * ═══════════════════════════════════════════════
 *
 * Prerequisites
 * ─────────────
 *   docker-compose -f docker-compose.demo.yml up -d
 *   # wait ~10 s for the broker to be ready, then:
 *   node examples/cooperativeStickyDemo.js
 *
 * What this script demonstrates
 * ─────────────────────────────
 *  Phase 1 - 4 consumers join.  All 10 partitions get distributed evenly.
 *
 *  Phase 2 - A 5th consumer joins.  Each of the existing consumers gives up
 *             ONE OR TWO partitions to the newcomer, but the rest of their
 *             partitions are NEVER revoked.  With an eager (roundRobin /
 *             standard sticky) assignor every consumer would have to stop
 *             processing for the full duration of the rebalance.  Here you
 *             can see in the per-consumer message counters that consumers
 *             whose partitions did NOT transfer keep incrementing without
 *             any gap.
 *
 *  Phase 3 - Consumer-1 is deliberately disconnected.  The remaining 4
 *             consumers each absorb one of its partitions in a second
 *             cooperative rebalance.  Again, consumers that are not involved
 *             in any transfer continue uninterrupted.
 */

'use strict'

const { Kafka, PartitionAssigners, Partitioners, logLevel } = require('../index')

// ─── Configuration ───────────────────────────────────────────────────────────

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
const TOPIC = process.env.DEMO_TOPIC || 'cooperative-sticky-demo'
const NUM_PARTITIONS = 10
const GROUP_ID = 'cooperative-demo-group'

// How often the producer sends a batch of messages (ms)
const PRODUCE_INTERVAL_MS = 300
// How many messages per produce cycle
const MESSAGES_PER_CYCLE = NUM_PARTITIONS

// How long to let each phase run before moving to the next one (ms)
const PHASE_SETTLE_MS = 8000

// ─── ANSI colour helpers ──────────────────────────────────────────────────────

const RESET = '\x1b[0m'
const BOLD = '\x1b[1m'
const DIM = '\x1b[2m'
const colours = [
  '\x1b[36m', // cyan   - C1
  '\x1b[32m', // green  - C2
  '\x1b[33m', // yellow - C3
  '\x1b[35m', // magenta– C4
  '\x1b[34m', // blue   - C5
]
const col = (i, s) => `${colours[i % colours.length]}${s}${RESET}`
const banner = s => `\n${BOLD}${'─'.repeat(60)}\n  ${s}\n${'─'.repeat(60)}${RESET}`
const ts = () => `${DIM}[${new Date().toISOString().slice(11, 23)}]${RESET}`

// ─── State tracking ───────────────────────────────────────────────────────────

/**
 * Per-consumer state.
 * @type {Map<string, { partitions: number[], msgCount: number, lastLog: number }>}
 */
const consumerState = new Map()

function printAssignmentTable(label) {
  console.log(`\n${BOLD}${label}${RESET}`)
  const rows = [...consumerState.entries()].sort(([a], [b]) => a.localeCompare(b))
  for (const [id, s] of rows) {
    const parts = [...s.partitions].sort((a, b) => a - b)
    const bar = parts
      .map(p =>
        col(
          rows.findIndex(([i]) => i === id),
          `[P${p}]`
        )
      )
      .join(' ')
    console.log(`  ${id.padEnd(14)} ${bar || '(none - waiting for transfer)'}`)
  }
  console.log()
}

// ─── Kafka setup ─────────────────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'cooperative-demo',
  brokers: BROKERS,
  logLevel: logLevel.WARN, // keep noise low; we do our own logging
})

// ─── Broker readiness ────────────────────────────────────────────────────────

/**
 * Poll until BOTH the broker AND the group coordinator are accepting requests.
 * The group coordinator (backed by __consumer_offsets) takes longer to start
 * than the broker itself, so we verify it explicitly via listGroups().
 */
async function waitForBroker(maxWaitMs = 90000) {
  const start = Date.now()
  let lastErr = null
  while (Date.now() - start < maxWaitMs) {
    const admin = kafka.admin()
    try {
      await admin.connect()
      await admin.listTopics() // broker up?
      await admin.listGroups() // group coordinator up?
      await admin.disconnect()
      return
    } catch (err) {
      lastErr = err
      await admin.disconnect().catch(() => {})
      await sleep(2000)
    }
  }
  throw new Error(
    `Kafka at ${BROKERS} not ready after ${maxWaitMs / 1000}s - last error: ${lastErr.message}`
  )
}

// ─── Topic setup ─────────────────────────────────────────────────────────────

async function ensureTopic() {
  const admin = kafka.admin()
  await admin.connect()

  const existing = await admin.listTopics()
  if (existing.includes(TOPIC)) {
    // Delete and recreate so we always start clean
    await admin.deleteTopics({ topics: [TOPIC] })
    await sleep(1500)
  }

  await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic: TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: 1 }],
  })
  console.log(`${ts()} Topic "${TOPIC}" ready with ${NUM_PARTITIONS} partitions`)
  await admin.disconnect()
}

// ─── Producer ────────────────────────────────────────────────────────────────

let msgSeq = 0
let producerHandle = null

async function startProducer() {
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  })
  await producer.connect()

  const intervalId = setInterval(async () => {
    const messages = Array.from({ length: MESSAGES_PER_CYCLE }, (_, i) => {
      const partitionKey = String(i) // key = '0'..'9' → routes to partition i
      return { key: partitionKey, value: `msg-${++msgSeq}` }
    })
    try {
      await producer.send({ topic: TOPIC, messages })
    } catch (_) {
      /* ignore transient errors during rebalance */
    }
  }, PRODUCE_INTERVAL_MS)

  producerHandle = { producer, intervalId }
  console.log(`${ts()} Producer started (${MESSAGES_PER_CYCLE} msgs / ${PRODUCE_INTERVAL_MS}ms)`)
  return producerHandle
}

async function stopProducer() {
  if (!producerHandle) return
  clearInterval(producerHandle.intervalId)
  await producerHandle.producer.disconnect()
  producerHandle = null
}

// ─── Consumer factory ─────────────────────────────────────────────────────────

/**
 * Creates, connects, subscribes and runs a consumer.
 * Blocks until the consumer has received its first GROUP_JOIN event, then
 * returns the consumer instance.  Setting up the join-promise BEFORE calling
 * consumer.run() avoids the race where the event fires before the caller can
 * attach a listener.
 *
 * @param {string} consumerId  Human-readable label, e.g. "C1"
 * @param {number} idx         Colour index
 */
async function createConsumer(consumerId, idx) {
  const state = { partitions: [], msgCount: 0, lastLog: Date.now() }
  consumerState.set(consumerId, state)

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    partitionAssigners: [PartitionAssigners.cooperativeSticky],
    sessionTimeout: 10000,
    heartbeatInterval: 1000,
    maxWaitTimeInMs: 500,
  })

  // ── Track assignment changes ──────────────────────────────────────────────
  consumer.on(consumer.events.GROUP_JOIN, event => {
    const assignment = event.payload.memberAssignment // { [topic]: number[] }
    const prev = state.partitions
    const next = Object.values(assignment)
      .flat()
      .map(Number)
      .sort((a, b) => a - b)
    const gained = next.filter(p => !prev.includes(p))
    const lost = prev.filter(p => !next.includes(p))
    state.partitions = next

    console.log(
      `${ts()} ${col(idx, consumerId)} joined group  ` +
        `partitions=${next.map(p => `P${p}`).join(',')} ` +
        (gained.length ? `${BOLD}+[${gained.map(p => `P${p}`).join(',')}]${RESET} ` : '') +
        (lost.length ? `${DIM}-[${lost.map(p => `P${p}`).join(',')}]${RESET} ` : '')
    )
  })

  // Set up first-join promise BEFORE consumer.run() so we never miss the event.
  // consumer.on() returns a removeListener fn; use it to simulate "once" semantics.
  const firstJoin = new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${consumerId}: timeout waiting for first GROUP_JOIN`)),
      40000
    )
    const removeListener = consumer.on(consumer.events.GROUP_JOIN, () => {
      clearTimeout(timer)
      removeListener()
      resolve()
    })
  })

  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false })

  await consumer.run({
    eachMessage: async () => {
      state.msgCount++
      const now = Date.now()
      // Log throughput briefly every 3 s so output doesn't flood
      if (now - state.lastLog >= 3000) {
        console.log(
          `${ts()} ${col(idx, consumerId)} ` +
            `partitions=[${[...state.partitions]
              .sort((a, b) => a - b)
              .map(p => `P${p}`)
              .join(',')}]  ` +
            `msgs=${state.msgCount}`
        )
        state.lastLog = now
      }
    },
  })

  // Wait until this consumer has actually joined the group before returning
  await firstJoin
  console.log(`${ts()} ${col(idx, consumerId)} consumer ready`)
  return consumer
}

/** Simple sleep helper */
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

// ─── Main demo ────────────────────────────────────────────────────────────────

async function main() {
  console.log(banner('Cooperative Sticky Assignor - Live Demo'))
  console.log(`  Broker : ${BROKERS.join(', ')}`)
  console.log(`  Topic  : ${TOPIC}  (${NUM_PARTITIONS} partitions)`)
  console.log(`  Group  : ${GROUP_ID}`)

  // ── Setup ──────────────────────────────────────────────────────────────────
  console.log(`${ts()} Waiting for broker to be ready…`)
  await waitForBroker()
  console.log(`${ts()} Broker is ready`)
  await ensureTopic()
  await startProducer()

  // ── Phase 1: start 4 consumers ─────────────────────────────────────────────
  console.log(banner('Phase 1 - Starting 4 consumers'))

  const consumers = []

  for (let i = 0; i < 4; i++) {
    // createConsumer blocks until the consumer has joined the group
    const c = await createConsumer(`C${i + 1}`, i)
    consumers.push(c)
  }

  // All 4 have joined; give any pending second cooperative round a moment to settle
  await sleep(2000)

  printAssignmentTable('Phase 1 - Steady state (4 consumers, 10 partitions)')
  console.log(
    `  Each consumer should hold ~2-3 partitions.\n` +
      `  Messages are flowing continuously. Letting it run for ${PHASE_SETTLE_MS / 1000}s…`
  )
  await sleep(PHASE_SETTLE_MS)

  // ── Phase 2: add a 5th consumer ────────────────────────────────────────────
  console.log(
    banner(
      'Phase 2 - Adding Consumer 5\n' +
        '  (watch: only the 1-2 partitions being TRANSFERRED will pause;\n' +
        '   all others keep receiving without interruption)'
    )
  )

  const c5 = await createConsumer('C5', 4)
  consumers.push(c5)
  await sleep(3000) // let second round of cooperative rebalance finish

  printAssignmentTable('Phase 2 - After C5 joined (5 consumers, 10 partitions)')
  console.log(
    `  C5 should now hold ~2 partitions.\n` +
      `  The snapshot below shows message counts - note that some consumers\n` +
      `  have been receiving messages non-stop while others briefly paused:\n`
  )

  for (const [id, s] of [...consumerState.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const idx = [...consumerState.keys()].sort().indexOf(id)
    console.log(`  ${col(idx, id.padEnd(4))} total messages processed: ${s.msgCount}`)
  }

  console.log(`\n  Letting it run for ${PHASE_SETTLE_MS / 1000}s…`)
  await sleep(PHASE_SETTLE_MS)

  // ── Phase 3: disconnect Consumer 1 ─────────────────────────────────────────
  console.log(
    banner(
      'Phase 3 - Disconnecting Consumer 1\n' +
        '  (its partitions will be redistributed among C2-C5)'
    )
  )

  const c1State = consumerState.get('C1')
  const c1Partitions = [...c1State.partitions].sort((a, b) => a - b)
  console.log(`${ts()} C1 currently owns [${c1Partitions.map(p => `P${p}`).join(', ')}]`)
  console.log(`${ts()} Disconnecting C1…`)

  await consumers[0].disconnect()
  consumerState.delete('C1')

  // Wait for the remaining consumers to re-join after C1 leaves
  const rebalancePromises = consumers.slice(1).map(
    c =>
      new Promise(resolve => {
        const remove = c.on(c.events.GROUP_JOIN, event => {
          remove()
          resolve(event)
        })
      })
  )
  await Promise.all(rebalancePromises)
  await sleep(3000)

  printAssignmentTable('Phase 3 - After C1 left (4 consumers, 10 partitions)')
  console.log(`  C1's ${c1Partitions.length} partitions have been distributed among C2-C5.`)
  console.log(`  Consumers whose partition sets did NOT change never stopped processing.\n`)

  for (const [id, s] of [...consumerState.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const idx = [...consumerState.keys()].sort().indexOf(id)
    console.log(`  ${col(idx, id.padEnd(4))} total messages processed: ${s.msgCount}`)
  }

  await sleep(PHASE_SETTLE_MS)

  // ── Teardown ───────────────────────────────────────────────────────────────
  console.log(banner('Demo complete - cleaning up'))
  await stopProducer()
  await Promise.all(consumers.slice(1).map(c => c.disconnect()))
  console.log('All done.')
  process.exit(0)
}

// ─── Error / signal handling ──────────────────────────────────────────────────

process.on('SIGINT', async () => {
  console.log('\nInterrupted, cleaning up…')
  await stopProducer()
  process.exit(0)
})

main().catch(err => {
  console.error('Demo failed:', err)
  process.exit(1)
})
