const waitFor = require('../../utils/waitFor')
const InstrumentationEmitter = require('../../instrumentation/emitter')
const fetchManager = require('../fetchManager')
const Batch = require('../batch')
const sleep = require('../../utils/sleep')

describe('FetchManager', () => {
  const nodeIds = [0, 1, 2]
  const fetch = jest.fn(async () => [])
  const topicPartitions = [
    { topic: 'topic1', partitions: [0] },
    { topic: 'topic2', partitions: [0, 1, 2] },
  ]
  let manager

  const createManager = overrides => {
    return fetchManager({
      instrumentationEmitter: new InstrumentationEmitter(),
      concurrency: 2,
      nodeIds,
      fetch,
      ...overrides,
    })
  }

  beforeEach(() => {
    manager = createManager({ fetch })
    manager.assign(topicPartitions)
  })

  describe('assign()', () => {
    it('should assign partitions to a single runner', () => {
      manager = createManager({ concurrency: 1 })
      const assignments = manager.assign(topicPartitions)
      expect(assignments).toEqual({
        topic1: { 0: 0 },
        topic2: { 0: 0, 1: 0, 2: 0 },
      })
    })

    it('should assign partitions evenly', () => {
      manager = createManager({ concurrency: 2 })
      const assignments = manager.assign(topicPartitions)
      expect(assignments).toEqual({
        topic1: { 0: 0 },
        topic2: { 0: 1, 1: 0, 2: 1 },
      })
    })

    it('should assign partitions to the first runners', () => {
      manager = createManager({ concurrency: 6 })
      const assignments = manager.assign(topicPartitions)
      expect(assignments).toEqual({
        topic1: { 0: 0 },
        topic2: { 0: 1, 1: 2, 2: 3 },
      })
    })
  })

  describe('next()', () => {
    const callback = jest.fn()
    const batch = new Batch('topic1', 0, {
      partition: 0,
      highWatermark: '100',
      messages: [{ offset: '1' }, { offset: '2' }],
    })

    beforeEach(() => {
      callback.mockReset()
    })

    it('should fetch all nodes on first next()', async () => {
      await manager.next({ runnerId: 0, callback })

      await waitFor(() => fetch.mock.calls.length >= 3)
      expect(fetch).toHaveBeenCalledTimes(3)

      nodeIds.forEach(nodeId => expect(fetch).toHaveBeenCalledWith(nodeId))
    })

    it('should throw error from any previous concurrent fetch', async () => {
      const error = new Error('💣')
      const fetch = jest.fn(async nodeId => {
        if (nodeId === 1) {
          await sleep(50)
          throw error
        }
        return []
      })

      manager = createManager({ fetch })
      await manager.next({ runnerId: 0, callback })
      await waitFor(() => fetch.mock.calls.length >= 3)

      await expect(manager.next({ runnerId: 0, callback })).rejects.toThrow(error)
    })

    it('should wait for the first fetch response when queue is empty', async () => {
      const fetch = jest.fn(async () => {
        await sleep(100)
        return [batch]
      })
      manager = createManager({ fetch })
      manager.assign(topicPartitions)
      await manager.next({ runnerId: 0, callback })

      expect(callback).toHaveBeenCalledWith(batch)
    })

    it('should re-fetch after all messages for a single node are processed', async () => {
      const fetch = jest.fn(async nodeId => {
        await sleep(nodeId === 2 ? 50 : 1000)
        return []
      })
      manager = createManager({ fetch })
      await manager.next({ runnerId: 0, callback })
      await manager.next({ runnerId: 0, callback })
      expect(fetch).toHaveBeenCalledTimes(4) // first all 3 nodeIds, then only nodeId 2
    })

    it('should not re-fetch messages if fetch is already in progress', async () => {
      const fetch = jest.fn(async () => {
        await sleep(100)
        return []
      })
      manager = createManager({ fetch, nodeIds: [0], concurrency: 1 })

      manager.next({ runnerId: 0, callback })
      await sleep(10)
      await manager.next({ runnerId: 0, callback })

      expect(fetch).toHaveBeenCalledTimes(1)
    })

    it('should not re-fetch if a batch from node is being processed', async () => {
      const fetch = jest.fn(async () => [batch])
      const callback = jest.fn(() => sleep(100))

      manager = createManager({ fetch, nodeIds: [0], concurrency: 1 })
      manager.assign(topicPartitions)

      manager.next({ runnerId: 0, callback })
      await sleep(10)
      await manager.next({ runnerId: 0, callback })

      expect(fetch).toHaveBeenCalledTimes(1)
    })
  })
})
