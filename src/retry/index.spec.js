const retrier = require('./index')

describe('Retry', () => {
  it('never schedules a negative timeout delay', async () => {
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout')
    const retry = retrier({ initialRetryTime: 1, factor: 0, multiplier: 1, retries: 1 })

    let attempts = 0
    await retry(async () => {
      attempts++
      if (attempts === 1) {
        await new Promise(resolve => setTimeout(resolve, 15))
        throw new Error('fail once')
      }
      return 'ok'
    })

    const delays = setTimeoutSpy.mock.calls.map(call => call[1])
    expect(delays.every(delay => delay >= 0)).toBe(true)
    setTimeoutSpy.mockRestore()
  })
})
