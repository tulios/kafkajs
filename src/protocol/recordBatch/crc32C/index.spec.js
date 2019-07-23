const crc32C = require('./index')

describe('Protocol > RecordBatch > crc32C', () => {
  test('perform CRC32C computations', () => {
    const longString =
      'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi mollis cursus metus vel tristique. Proin congue massa massa, a malesuada dolor ullamcorper a. Nulla eget leo vel orci venenatis placerat. Donec semper condimentum justo, vel sollicitudin dolor consequat id. Nunc sed aliquet felis, eget congue nisi. Mauris eu justo suscipit, elementum turpis ut, molestie tellus. Mauris ornare rutrum fringilla. Nulla dignissim luctus pretium. Nullam nec eros hendrerit sapien pellentesque sollicitudin. Integer eget ligula dui. Mauris nec cursus nibh. Nunc interdum elementum leo, eu sagittis eros sodales nec. Duis dictum nulla sed tincidunt malesuada. Quisque in vulputate sapien. Sed sit amet tellus a est porta rhoncus sed eu metus. Mauris non pulvinar nisl, volutpat luctus enim. Suspendisse est nisi, sagittis at risus quis, ultricies rhoncus sem. Donec ullamcorper purus eget sapien facilisis, eu eleifend felis viverra. Suspendisse elit neque, semper aliquet neque sed, egestas tempus leo. Duis condimentum turpis duis.'
    const buffer = Buffer.from(longString)
    expect(crc32C(buffer)).toEqual(1796588439)
  })

  test('match the java CRC32C code', () => {
    const buffer = Buffer.from(require('./fixtures/crcPayload.json'))
    expect(crc32C(buffer)).toEqual(818496390)
  })

  test('samples', () => {
    const samples = require('./fixtures/samples')
    for (const sample of samples) {
      const buffer = Buffer.from(sample.input)
      expect(crc32C(buffer)).toEqual(sample.output)
    }
  })

  test('empty', () => {
    expect(crc32C(Buffer.alloc(0))).toEqual(0)
  })

  test('unicode null', () => {
    expect(crc32C(Buffer.from('\u0000'))).toEqual(1383945041)
  })
})
