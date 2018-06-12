const waitFor = require('./waitFor')

describe('Utils > waitFor', () => {
  it('waits for the condition', async () => {
    let conditionValid = false

    setTimeout(() => {
      conditionValid = true
    }, 6)

    await expect(waitFor(() => conditionValid, { delay: 5 }))
  })

  it('rejects the promise if the callback fail', async () => {
    await expect(
      waitFor(
        () => {
          throw new Error('callback failed!')
        },
        { delay: 1 }
      )
    ).rejects.toHaveProperty('message', 'callback failed!')
  })

  it('rejects the promise if the callback never succeeds', async () => {
    await expect(waitFor(() => false, { delay: 1, maxWait: 2 })).rejects.toHaveProperty(
      'message',
      'Timeout'
    )
  })

  it('rejects the promise with a custom timeout message', async () => {
    await expect(
      waitFor(() => false, { delay: 1, maxWait: 2, timeoutMessage: 'foo bar' })
    ).rejects.toHaveProperty('message', 'foo bar')
  })
})
