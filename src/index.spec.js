const Client = require('./index')

describe('Client', () => {
  it('gives access to its logger', () => {
    expect(new Client({ brokers: [] }).logger()).toMatchSnapshot()
  })
})
