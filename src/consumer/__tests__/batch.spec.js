const Batch = require('../batch')

describe('Consumer > Batch', () => {
  const topic = 'topic-name'

  it('discards messages with a lower offset than the requested', () => {
    const fetchedOffset = 3
    const batch = new Batch(topic, fetchedOffset, {
      partition: 0,
      highWatermark: '100',
      messages: [
        { offset: '0' },
        { offset: '1' },
        { offset: '2' },
        { offset: '3' },
        { offset: '4' },
        { offset: '5' },
      ],
    })

    expect(batch.messages).toEqual([{ offset: '3' }, { offset: '4' }, { offset: '5' }])
  })

  it('discards control records', () => {
    const fetchedOffset = 0
    const batch = new Batch(topic, fetchedOffset, {
      partition: 0,
      highWatermark: '100',
      messages: [{ offset: '3', isControlRecord: true }, { offset: '4' }, { offset: '5' }],
    })

    expect(batch.messages).toEqual([{ offset: '4' }, { offset: '5' }])
  })

  describe('#isEmpty', () => {
    it('returns true when empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.isEmpty()).toEqual(true)
    })

    it('returns false when it has messages', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.isEmpty()).toEqual(false)
    })
  })

  describe('#firstOffset', () => {
    it('returns the offset of the first message', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.firstOffset()).toEqual('1')
    })

    it('returns null when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.firstOffset()).toEqual(null)
    })
  })

  describe('#lastOffset', () => {
    it('returns the offset of the last message', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.lastOffset()).toEqual('2')
    })

    it('returns highWatermark - 1 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.lastOffset()).toEqual('99')
    })
  })

  describe('#offsetLag', () => {
    it('returns the difference between highWatermark - 1 and the last offset', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.offsetLag()).toEqual('95')
    })

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.offsetLag()).toEqual('0')
    })
  })

  describe('#offsetLagLow', () => {
    it('returns the difference between highWatermark - 1 and the first offset', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.offsetLagLow()).toEqual('96')
    })

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.offsetLagLow()).toEqual('0')
    })
  })

  describe('#isEmptyControlRecord', () => {
    it('returns false for regular batches', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns false for regular empty batches', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns false if there is a control record but some messages are available', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }, { isControlRecord: true, offset: '5' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns true if the batch only contains a control record', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ isControlRecord: true, offset: '5' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(true)
    })
  })
})
