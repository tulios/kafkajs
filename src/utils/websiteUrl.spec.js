const websiteUrl = require('./websiteUrl')

describe('Utils > websiteUrl', () => {
  it('generates links to the website', () => {
    expect(websiteUrl('docs/faq')).toEqual('https://kafka.js.org/docs/faq')
  })

  it('allows specifying a hash', () => {
    expect(
      websiteUrl('docs/faq', 'why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to')
    ).toEqual(
      'https://kafka.js.org/docs/faq#why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'
    )
  })
})
