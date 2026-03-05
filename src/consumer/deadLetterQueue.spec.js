const createDLQ = require('./deadLetterQueue')

describe('deadLetterQueue', () => {
  let producer, sendMock

  beforeEach(() => {
    sendMock = jest.fn().mockResolvedValue([])
    producer = { send: sendMock }
  })

  it('throws if producer is not provided', () => {
    expect(() => createDLQ({ topic: 'dlq' })).toThrow('requires a producer')
  })

  it('throws if topic is not provided', () => {
    expect(() => createDLQ({ producer })).toThrow('requires a topic')
  })

  it('calls the handler normally when it succeeds', async () => {
    const handler = jest.fn().mockResolvedValue(undefined)
    const withDLQ = createDLQ({ producer, topic: 'dlq' })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 'test-topic',
      partition: 0,
      message: {
        key: null,
        value: Buffer.from('hello'),
        offset: '0',
        timestamp: '123',
        headers: {},
      },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    expect(handler).toHaveBeenCalledWith(payload)
    expect(sendMock).not.toHaveBeenCalled()
  })

  it('retries the handler up to maxRetries before sending to DLQ', async () => {
    const error = new Error('processing failed')
    const handler = jest.fn().mockRejectedValue(error)
    const withDLQ = createDLQ({ producer, topic: 'dlq', maxRetries: 2 })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 'test-topic',
      partition: 1,
      message: {
        key: Buffer.from('key1'),
        value: Buffer.from('value1'),
        offset: '5',
        timestamp: '999',
        headers: { 'x-custom': 'abc' },
      },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    // 1 initial + 2 retries = 3 calls
    expect(handler).toHaveBeenCalledTimes(3)
    expect(sendMock).toHaveBeenCalledWith({
      topic: 'dlq',
      messages: [
        {
          key: Buffer.from('key1'),
          value: Buffer.from('value1'),
          headers: {
            'x-custom': 'abc',
            'dlq.original.topic': 'test-topic',
            'dlq.original.partition': '1',
            'dlq.original.offset': '5',
            'dlq.original.timestamp': '999',
            'dlq.error.message': 'processing failed',
            'dlq.error.name': 'Error',
          },
        },
      ],
    })
  })

  it('uses default maxRetries of 3', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('fail'))
    const withDLQ = createDLQ({ producer, topic: 'dlq' })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 't',
      partition: 0,
      message: { key: null, value: null, offset: '0', timestamp: '0', headers: {} },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    // 1 initial + 3 retries = 4 calls
    expect(handler).toHaveBeenCalledTimes(4)
    expect(sendMock).toHaveBeenCalledTimes(1)
  })

  it('calls onOriginalMessageFailed callback after sending to DLQ', async () => {
    const error = new Error('fail')
    const handler = jest.fn().mockRejectedValue(error)
    const onFailed = jest.fn().mockResolvedValue(undefined)
    const withDLQ = createDLQ({
      producer,
      topic: 'dlq',
      maxRetries: 0,
      onOriginalMessageFailed: onFailed,
    })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 't',
      partition: 0,
      message: { key: null, value: null, offset: '0', timestamp: '0', headers: {} },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    expect(onFailed).toHaveBeenCalledWith(payload, error)
  })

  it('uses createDLQMessage when provided', async () => {
    const error = new Error('custom fail')
    const handler = jest.fn().mockRejectedValue(error)
    const createDLQMessage = jest.fn().mockReturnValue({
      key: 'custom-key',
      value: 'custom-value',
      headers: { custom: 'header' },
    })

    const withDLQ = createDLQ({ producer, topic: 'dlq', maxRetries: 0, createDLQMessage })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 't',
      partition: 0,
      message: { key: null, value: null, offset: '0', timestamp: '0', headers: {} },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    expect(createDLQMessage).toHaveBeenCalledWith(error, payload)
    expect(sendMock).toHaveBeenCalledWith({
      topic: 'dlq',
      messages: [{ key: 'custom-key', value: 'custom-value', headers: { custom: 'header' } }],
    })
  })

  it('succeeds on retry without sending to DLQ', async () => {
    let callCount = 0
    const handler = jest.fn().mockImplementation(async () => {
      callCount++
      if (callCount < 3) throw new Error('transient')
    })

    const withDLQ = createDLQ({ producer, topic: 'dlq', maxRetries: 3 })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 't',
      partition: 0,
      message: { key: null, value: null, offset: '0', timestamp: '0', headers: {} },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await wrapped(payload)

    expect(handler).toHaveBeenCalledTimes(3)
    expect(sendMock).not.toHaveBeenCalled()
  })

  it('propagates error if DLQ produce fails', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('handler fail'))
    sendMock.mockRejectedValue(new Error('produce fail'))

    const withDLQ = createDLQ({ producer, topic: 'dlq', maxRetries: 0 })
    const wrapped = withDLQ(handler)

    const payload = {
      topic: 't',
      partition: 0,
      message: { key: null, value: null, offset: '0', timestamp: '0', headers: {} },
      heartbeat: jest.fn(),
      pause: jest.fn(),
    }

    await expect(wrapped(payload)).rejects.toThrow('produce fail')
  })
})
