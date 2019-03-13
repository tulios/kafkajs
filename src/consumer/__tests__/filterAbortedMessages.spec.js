const abortedBatch = require('./fixtures/batches/aborted')
const committedBatch = require('./fixtures/batches/committed')
const nontransactionalBatch = require('./fixtures/batches/nontransactional')
const filterAbortedMessages = require('../filterAbortedMessages')

describe('filterAbortedMessages', () => {
  test('filters out all aborted messages', () => {
    const { messages, abortedTransactions } = abortedBatch
    const { messages: nontransactionalMessages } = nontransactionalBatch

    expect(messages).toHaveLength(3)
    expect(abortedTransactions).toHaveLength(1)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]), // Abort control message
      }),
    ])

    expect(
      filterAbortedMessages({
        messages: [...messages, ...nontransactionalMessages],
        abortedTransactions,
      })
    ).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]),
      }),
      ...nontransactionalMessages,
    ])
  })

  test('filters out aborted messages with malformed keys', () => {
    const { messages, abortedTransactions } = abortedBatch
    const { messages: nontransactionalMessages } = nontransactionalBatch
    messages[messages.length - 2].key = null
    expect(
      filterAbortedMessages({
        messages: [...messages, ...nontransactionalMessages],
        abortedTransactions,
      })
    ).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]),
      }),
      ...nontransactionalMessages,
    ])
  })

  test('returns all committed messages', () => {
    const { messages, abortedTransactions } = committedBatch
    expect(messages).toHaveLength(4)
    expect(abortedTransactions).toHaveLength(0)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual(messages)
  })

  test('returns all nontransactional messages', () => {
    const { messages, abortedTransactions } = nontransactionalBatch
    expect(messages).toHaveLength(3)
    expect(abortedTransactions).toHaveLength(0)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual(messages)
  })
})
