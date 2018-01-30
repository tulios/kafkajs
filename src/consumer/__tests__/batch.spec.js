const Batch = require('../batch')

describe('Consumer > Batch', () => {
  const topic = 'topic-name'

  describe('#isEmpty', () => {
    it('returns true when empty', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.isEmpty()).toEqual(true)
    })

    it('returns false when it has messages', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [{}, {}],
      })

      expect(batch.isEmpty()).toEqual(false)
    })
  })

  describe('#firstOffset', () => {
    it('returns the offset of the first message', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.firstOffset()).toEqual('1')
    })

    it('returns null when the batch is empty', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.firstOffset()).toEqual(null)
    })
  })

  describe('#lastOffset', () => {
    it('returns the offset of the last message', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.lastOffset()).toEqual('2')
    })

    it('returns highWatermark - 1 when the batch is empty', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.lastOffset()).toEqual('99')
    })
  })

  describe('#offsetLag', () => {
    it('returns the difference between highWatermark - 1 and the last offset', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.offsetLag()).toEqual('95')
    })

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.offsetLag()).toEqual('0')
    })
  })
})
