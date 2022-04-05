const { versions, protocol } = require('./index')

describe('Protocol > Requests > Produce', () => {
  versions.forEach(version => {
    describe(`v${version}`, () => {
      test('metadata about the API', () => {
        const { request } = protocol({ version })({})
        expect(request.expectResponse()).toEqual(true)
      })
    })
  })
})
