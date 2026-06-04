const { createCluster, secureRandom } = require('testHelpers')
const { createErrorFromCode } = require('../../protocol/error')

const INVALID_TOPIC_EXCEPTION = 17

describe('Cluster > addMultipleTargetTopics (per-topic memoization)', () => {
  let cluster

  beforeEach(() => {
    cluster = createCluster()
    cluster.brokerPool.metadata = { some: 'metadata' }
  })

  test('no global lock instance exists on the cluster', () => {
    expect(cluster.mutatingTargetTopics).toBeUndefined()
  })

  test('concurrent calls for different new topics do not serialize', async () => {
    const topic1 = `topic-${secureRandom()}`
    const topic2 = `topic-${secureRandom()}`
    let inFlight = 0
    let maxInFlight = 0
    cluster.refreshMetadata = jest.fn(async () => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise(resolve => setTimeout(resolve, 10))
      inFlight--
    })

    await Promise.all([
      cluster.addMultipleTargetTopics([topic1]),
      cluster.addMultipleTargetTopics([topic2]),
    ])

    expect(maxInFlight).toBe(2)
  })

  test('concurrent calls for the same topic share a single in-flight refresh', async () => {
    const topic = `topic-${secureRandom()}`
    let refreshes = 0
    cluster.refreshMetadata = jest.fn(async () => {
      refreshes++
      await new Promise(resolve => setTimeout(resolve, 10))
    })

    await Promise.all([
      cluster.addMultipleTargetTopics([topic]),
      cluster.addMultipleTargetTopics([topic]),
      cluster.addMultipleTargetTopics([topic]),
    ])

    expect(refreshes).toBe(1)
  })

  test('a second call after the first resolved triggers a new refresh only if needed', async () => {
    const topic = `topic-${secureRandom()}`
    cluster.refreshMetadata = jest.fn()

    await cluster.addMultipleTargetTopics([topic])
    await cluster.addMultipleTargetTopics([topic])

    // topic is now in targetTopics and brokerPool.metadata is still truthy,
    // so the second call short-circuits.
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('on INVALID_TOPIC_EXCEPTION, removes only the topic this call newly added', async () => {
    const preexisting = `topic-${secureRandom()}`
    const failing = `topic-${secureRandom()}`
    cluster.targetTopics.add(preexisting)
    cluster.refreshMetadata = jest
      .fn()
      .mockRejectedValue(createErrorFromCode(INVALID_TOPIC_EXCEPTION))

    await expect(
      cluster.addMultipleTargetTopics([preexisting, failing])
    ).rejects.toHaveProperty('type', 'INVALID_TOPIC_EXCEPTION')

    expect(cluster.targetTopics.has(preexisting)).toBe(true)
    expect(cluster.targetTopics.has(failing)).toBe(false)
  })

  test('non-topic-level errors propagate without rolling back targetTopics', async () => {
    const topic = `topic-${secureRandom()}`
    cluster.refreshMetadata = jest.fn().mockRejectedValue(new Error('boom'))

    await expect(cluster.addMultipleTargetTopics([topic])).rejects.toThrow('boom')

    expect(cluster.targetTopics.has(topic)).toBe(true)
  })

  test('clears the in-flight entry after the refresh resolves', async () => {
    const topic = `topic-${secureRandom()}`
    cluster.refreshMetadata = jest.fn()

    await cluster.addMultipleTargetTopics([topic])

    expect(cluster.inFlightMetadataByTopic.has(topic)).toBe(false)
  })

  test('clears the in-flight entry after the refresh rejects', async () => {
    const topic = `topic-${secureRandom()}`
    cluster.refreshMetadata = jest.fn().mockRejectedValue(new Error('boom'))

    await expect(cluster.addMultipleTargetTopics([topic])).rejects.toThrow('boom')

    expect(cluster.inFlightMetadataByTopic.has(topic)).toBe(false)
  })
})
