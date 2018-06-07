const { decode, parse } = require('./response')

describe('Protocol > sasl > scram > firstMessage', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('./fixtures/response.json')))
    await expect(parse(data)).resolves.toEqual({
      original:
        'r=IQi00EZwusKw0Io7FoBfqg1c7im78cnh566cwt0watlspw4p,s=bHcyM3p5bWk5aXF4OWM3cmswZHM5N2w0cA==,i=8192',
      r: 'IQi00EZwusKw0Io7FoBfqg1c7im78cnh566cwt0watlspw4p',
      s: 'bHcyM3p5bWk5aXF4OWM3cmswZHM5N2w0cA==',
      i: '8192',
    })
  })
})
