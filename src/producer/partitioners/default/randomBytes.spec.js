const { KafkaJSNonRetriableError } = require('../../../errors')
const randomBytes = require('./randomBytes')

describe('Producer > Partitioner > Default > RandomBytes', () => {
  test('it throws when requesting more bytes than entry allows', () => {
    expect(() => randomBytes(65537)).toThrowError(
      new KafkaJSNonRetriableError(
        'Byte length (65537) exceeds the max number of bytes of entropy available (65536)'
      )
    )
  })

  test('it returns random bytes of the desired length', () => {
    const bytes = randomBytes(32)
    expect(bytes).toEqual(expect.any(Buffer))
    expect(bytes.byteLength).toBe(32)
  })
})
